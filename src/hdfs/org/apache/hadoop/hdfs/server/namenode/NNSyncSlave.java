package org.apache.hadoop.hdfs.server.namenode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.EOFException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Daemon;

import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.*;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.protocol.*;

public class NNSyncSlave extends NNSyncer {

	NNCProtocol master;
	InetSocketAddress selfAddr;
	ServerSocket syncReceiver;
	
	String machineName=null;
	NNSlaveInfo slaveInfo;
	
	//status
	volatile boolean shouldRun = true;
	
	class SyncListener implements Runnable{

                  public void run(){
                      Socket s = null;
                      DataInputStream in = null;
                      DataOutputStream out = null;
		      try {
				s = syncReceiver.accept();
				s.setTcpNoDelay(true);
				in=new DataInputStream(new BufferedInputStream(
		        		NetUtils.getInputStream(s), FSConstants.BUFFER_SIZE));
				out = new DataOutputStream(new BufferedOutputStream(
				        NetUtils.getOutputStream(s, socketWriteTimeout), FSConstants.SMALL_BUFFER_SIZE));
				
				LOG.info("nnc slave SyncListener, slave Sync Receive Server accept connection of a master sync, L<-R " + s.getLocalSocketAddress().toString()+" <- " + s.getRemoteSocketAddress().toString());
				while (shouldRun) {
					try {
						if(in==null || out==null){
							in=new DataInputStream(new BufferedInputStream(
					        		NetUtils.getInputStream(s), FSConstants.BUFFER_SIZE));
							out = new DataOutputStream(new BufferedOutputStream(
							        NetUtils.getOutputStream(s, socketWriteTimeout), FSConstants.SMALL_BUFFER_SIZE));
						}
						
						NNUpdateInfo head=readUpdateHead(in);
						
						switch (head.updateFlag) {
						case NNUpdateInfo.NNF_WORLD:
							LOG.info("SyncListener: begin get world");
							recvWholeWorld(in,out,head);
							break;
						case NNUpdateInfo.NNF_MASS:
							LOG.info("SyncListener: begin get mass sync");
							recvMassUpdate(in,out,head);
							break;
						case NNUpdateInfo.NNF_UPDATE:
							LOG.debug("SyncListener: begin get regular sync");
							recvOneUpdate(in,out,head);
							break;
						// case NNSyncer.NNUS_NA:
						}
					} catch (EOFException eofe) {  //this catch added by rpy
						LOG.warn("EOFException occured when receiving and updating, Master NameNode has been shutdown!");
						if(in != null) {
							try{ in.close(); } catch(IOException iex) {}
							in = null;
						}
						if(out != null) {
							try{ out.close(); } catch(IOException iex) {}
							out = null;
						}
						
						try{ s.close(); } catch(IOException iex){}
						
						LOG.info("nnc slave SyncListener, slave Sync Receive Server close connection of a master sync, L<-R " + s.getLocalSocketAddress().toString()+" <- " + s.getRemoteSocketAddress().toString());
						
						shouldRun = false;
						
					} catch (IOException ie) {
						LOG.warn("exception occured when receiving and updating: "
										,ie);
					} catch (Throwable t) {
						LOG.warn("unknown exception occured when receiving and updating: "
										, t);
					} finally {
						// s.close();
					}
				}
				
				//in.close();  //commented by rpy
				//out.close(); //commented by rpy
				
				// syncReceiver.close();
			} catch (ClosedByInterruptException ce) {
				LOG.warn("session closed by interrupt, will be promoted to master?",ce);
			} catch (IOException ie) {
				LOG.warn(":Exiting SyncXceiveServer due to ",ie);
			} finally {
				if(in != null) {
					try{ in.close(); } catch(IOException iex) {}
					in = null;
				}
				if(out != null) {
					try{ out.close(); } catch(IOException iex) {}
					out = null;
				}
				if(s != null){
					try{ s.close(); } catch(IOException iex){}
				}
				
			}
		}
		
	}
	
	Daemon syncListener;

	public NNSyncSlave(NameNode nn, FSNamesystem namesystem, Configuration conf) throws IOException{
			super(nn,namesystem,conf);
			initialize(conf);
	}
	
	private void initialize(Configuration conf) throws IOException{
	    if (machineName == null) {
	        machineName = DNS.getDefaultHost(
	                                       conf.get("dfs.nnsyncslave.dns.interface","default"),
	                                       conf.get("dfs.nnsyncslave.dns.nameserver","default"));
	    }

	    String address = conf.get("dfs.sync.address", "0.0.0.0:50072"); 
	    
	    String masterAddress=nnkeeper.syncGetMasterNameNode(10000L);
	    String masterHost=NNSyncer.hostFromString(masterAddress);
	    int syncMasterPort=conf.getInt("dfs.nnc.masterport", 50078);
	    syncMasterAddress=NetUtils.createSocketAddr(masterHost+':'+syncMasterPort);
	    
	    LOG.info("slave at " + machineName + " try to connect to " + syncMasterAddress.getHostName() + ":" + syncMasterAddress.getPort());
	    
	    InetSocketAddress socAddr = NetUtils.createSocketAddr(address);
	    int tmpPort = socAddr.getPort();
	      
	    selfAddr = new InetSocketAddress(machineName, tmpPort);

	    this.slaveInfo=new NNSlaveInfo(machineName+":"+tmpPort,"");
	    
	    connectAndVerifyMaster(conf);
	     
	    socketWriteTimeout = conf.getInt("dfs.sync.socket.write.timeout", HdfsConstants.WRITE_TIMEOUT);
	    syncReceiver = (socketWriteTimeout > 0) ? ServerSocketChannel.open().socket() : new ServerSocket();
	    Server.bind(syncReceiver, socAddr, 0);
	    syncReceiver.setReceiveBufferSize(FSConstants.DEFAULT_DATA_SOCKET_SIZE); 
	        // adjust machine name with the actual port
	    tmpPort = syncReceiver.getLocalPort();
	    selfAddr = new InetSocketAddress(syncReceiver.getInetAddress().getHostAddress(), tmpPort);
	    this.slaveInfo.setName(machineName + ":" + tmpPort);
	    
	    this.updateQueue=new TreeMap<String,NNUpdateEncap>();
	    
	    syncListener=new Daemon(new SyncListener());
	    syncListener.start();

	    LOG.info("Opened server at " + tmpPort);
	    
	    register();
	}
	
	public void changeSyncMaster(String newMaster) throws IOException{
		LOG.info("Changing to new Master: "+newMaster);
		
		syncMasterAddress = new InetSocketAddress(NNSyncer.hostFromString(newMaster),syncMasterAddress.getPort());
		
		shouldRun=false;
		syncListener.interrupt();
		
		Configuration conf=new Configuration();
		connectAndVerifyMaster(conf);
		
		shouldRun=true;
		syncListener.start();
		
		LOG.info("Opened server again");
		
		register();
	}
	
	private void connectAndVerifyMaster(Configuration conf) throws IOException {
		
	    master = (NNCProtocol) RPC.waitForProxy(NNCProtocol.class,
                NNCProtocol.versionID,
                syncMasterAddress, 
                conf);

		NamespaceInfo nsInfo=handshake();
		
		LOG.info("connect with the SyncMaster with namespace Info " + nsInfo.getNamespaceID());
		
		if( namesystem.dir.fsImage.getNamespaceID()==0 ){
			LOG.info("format my fsimage...");
			formatFSImage(conf, nsInfo.namespaceID);
		}else if( namesystem.dir.fsImage.getNamespaceID() == nsInfo.getNamespaceID()){
			if( slaveInfo != null && slaveInfo.getUpdateID() != "" ){
				LOG.info("namespace id matches and has updateID " + slaveInfo.getUpdateID());
			}else{
				LOG.info("namespace id matches without updateID, format it");
				formatFSImage(conf, nsInfo.namespaceID);
			}
			//if(this.updateQueue.lastKey())
		}else{
			LOG.error("namespace id does not match");
			throw new IOException("redirect to an server with different id!");
		}
	}
	

	private NamespaceInfo handshake() throws IOException {
		NamespaceInfo nsInfo;
		LOG.info("try to request the server for handshake...");
		while (shouldRun) {
		    try {
		        nsInfo = master.versionRequest();
		        return nsInfo;
		    } catch(SocketTimeoutException e) {  // master is busy
		        LOG.warn("Problem connecting to server: " + getSyncMasterAddr());
		        try {
		            Thread.sleep(1000);
		        } catch (InterruptedException ie) {}
		    }
		}
		return null;
	}
	
	private void formatFSImage(Configuration conf, int nsid) throws IOException{
		Collection<File> dataDirs=FSNamesystem.getNamespaceDirs(conf);
		Collection<File> editsDirs=FSNamesystem.getNamespaceEditsDirs(conf);

		namesystem.dir.fsImage.setStorageDirectories(dataDirs,editsDirs);
	    namesystem.dir.fsImage.format(nsid);
	    
	    namesystem.dir.loadFSImage(FSNamesystem.getNamespaceDirs(conf),FSNamesystem.getNamespaceEditsDirs(conf), StartupOption.REGULAR);
	}
	
	public InetSocketAddress getSyncMasterAddr() {
		return syncMasterAddress;
	}

	private void register() throws IOException {
		if (slaveInfo.getNNSlaveID().equals("")) {
		    setNewNNSlaveID(slaveInfo);
		}
		
		LOG.info("register slave "+slaveInfo.name + " / " + slaveInfo.getUpdateID());
		//slave id should have some common idea with nsinfo
		while(shouldRun) {
		    try {
		        // reset name to machineName. Mainly for web interface.
		        NNSlaveInfo newslaveInfo = master.register(slaveInfo);
		        slaveInfo=newslaveInfo;
		        break;
		    } catch(SocketTimeoutException e) {  // master is busy
		        LOG.warn("Problem connecting to server: " + getSyncMasterAddr());
		        try {
		          Thread.sleep(1000);
		        } catch (InterruptedException ie) {}
		    }
		}
	}
	
	  static void setNewNNSlaveID(NNSlaveID nnSlaveID) {
		    /* Return 
		     * "SL-randInt-ipaddr-currentTimeMillis"
		     * It is considered extermely rare for all these numbers to match
		     * on a different machine accidentally for the following 
		     * a) SecureRandom(INT_MAX) is pretty much random (1 in 2 billion), and
		     * b) Good chance ip address would be different, and
		     * c) Even on the same machine, Datanode is designed to use different ports.
		     * d) Good chance that these are started at different times.
		     * For a confict to occur all the 4 above have to match!.
		     * The format of this string can be changed anytime in future without
		     * affecting its functionality.
		     */
		    String ip = "unknownIP";
		    try {
		      ip = DNS.getDefaultIP("default");
		    } catch (UnknownHostException ignored) {
		      LOG.warn("Could not find ip address of \"default\" inteface.");
		    }
		    
		    int rand = 0;
		    try {
		      rand = SecureRandom.getInstance("SHA1PRNG").nextInt(Integer.MAX_VALUE);
		    } catch (NoSuchAlgorithmException e) {
		      LOG.warn("Could not use SecureRandom");
		      rand = R.nextInt(Integer.MAX_VALUE);
		    }
		    nnSlaveID.nnSlaveID = "SL-" + rand + "-"+ ip + "-" + nnSlaveID.getPort() + "-" + 
		                      System.currentTimeMillis();
	  }

	private void updateNewFile(NNUpdateEncap update) throws IOException{
		NNUStartFile uunit=(NNUStartFile)update.body;
		
		LOG.debug("received update: "+uunit.clientName+"..."+uunit.clientMachine+"..."+uunit.clientNode
				+"..."+uunit.preferredBlockSize+"..."+uunit.replication+"..."+uunit.src+"..."+uunit.timestamp);
		
		UserGroupInformation.setCurrentUGI(uunit.ugi);
		
		namesystem.startFile(uunit.src,uunit.perm,uunit.clientName,uunit.clientMachine,true,uunit.replication,uunit.preferredBlockSize);
		//TODO: boolean overwrite and timestamp
		
	}
	
	private void updateINode(NNUpdateEncap update) throws IOException{
		NNUINodeInfo info=(NNUINodeInfo)update.body;
		switch(info.op){
		case NNUINodeInfo.NNU_REP:
			namesystem.setReplication(info.src, info.replication);//it sync editlog already
			break;
		case NNUINodeInfo.NNU_PERM:
			namesystem.dir.setPermission(info.src, new FsPermission(info.perm));
			namesystem.getEditLog().logSync();
			break;
		case NNUINodeInfo.NNU_OWNER:
			namesystem.dir.setOwner(info.src, info.user, info.group);
			namesystem.getEditLog().logSync();
			break;
		}

	}
	
	private void updateBlock(NNUpdateEncap update) throws IOException{
		NNUpdateBlk blk=(NNUpdateBlk)update.body;
		if(blk.add==false && blk.dnodes ==null){
			LOG.debug("abandon block blk_"+blk.blk.getBlockId());
			
			INode f = namesystem.dir.getFileINode(blk.src);
			if(f == null || !f.isUnderConstruction())
				return;
			
			INodeFileUnderConstruction file=(INodeFileUnderConstruction)f;
		    if(file.clientName.equals(blk.holder)) //all of above error case should not appear, we just do double check
		    	namesystem.dir.removeBlock(blk.src, file, blk.blk);
		}else if(blk.add==false && blk.dnodes!=null){
			LOG.debug("block blk_"+blk.blk.getBlockId()+" tagged invalid");
			
			DatanodeDescriptor node=namesystem.datanodeMap.get(blk.dnodes.getFirst());
			namesystem.invalidateBlock(blk.blk, node);
		}else if(blk.add==true && !blk.holder.equals("")){
			LOG.debug("allocate block blk_"+blk.blk.getBlockId()+ " for file "+blk.src);
			
			INode[] pathINodes = namesystem.dir.getExistingPathINodes(blk.src);
			int inodesLen = pathINodes.length;
			
			INodeFileUnderConstruction file=(INodeFileUnderConstruction)pathINodes[inodesLen - 1];
		    if(!file.clientName.equals(blk.holder)) //all this and above error case should not appear, we just do double check
		    	return;
		    
		    namesystem.dir.addBlock(blk.src, pathINodes, blk.blk);
		    DatanodeDescriptor targets[]=new DatanodeDescriptor[blk.dnodes.size()];
		    int i=0;
		    for(Iterator<String> iter=blk.dnodes.iterator();iter.hasNext();i++){
		    	targets[i]=namesystem.datanodeMap.get(iter.next());
		    }
		    file.setTargets(targets);
		}else{
			LOG.error("Unhandled block update sync");
		}
	}
	
	private void updateCloseFile(NNUpdateEncap update) throws IOException{
		NNUCloseNewFile file=(NNUCloseNewFile)update.body;
		switch(file.op){
			case NNUCloseNewFile.NNU_ABDF:
				//namesystem.abandonFileInProgress(file.src, file.holder);
				LOG.warn("AbandonFileInProgress has been removed");
				break;
			case NNUCloseNewFile.NNU_CPLF:
				INode f = namesystem.dir.getFileINode(file.src);
				if(f == null || !f.isUnderConstruction())//shouldn't occur
					return;
				INodeFileUnderConstruction pendingFile=(INodeFileUnderConstruction)f;
			    INodeFile newFile = pendingFile.convertToInodeFile();
			    namesystem.dir.replaceNode(file.src, pendingFile, newFile);
			    namesystem.dir.closeFile(file.src, newFile);
			    namesystem.leaseManager.removeLease(pendingFile.clientName,file.src);
			    namesystem.getEditLog().logSync();

				break;
		}
	}

	private void updateMvRm(NNUpdateEncap update) throws IOException{
		NNUMvRm mvrm=(NNUMvRm)update.body;
		if(!mvrm.dst.equals("")){
			LOG.debug("mv file "+mvrm.src+" to "+mvrm.dst);
			namesystem.dir.renameTo(mvrm.src, mvrm.dst);
			namesystem.getEditLog().logSync();
		}else{
			LOG.debug("rm file "+mvrm.src);
			//rpy namesystem.deleteInternal(mvrm.src, false, false);
			namesystem.deleteInternal(mvrm.src, false);
			namesystem.getEditLog().logSync();
		}
	}

	private void updateMkdirs(NNUpdateEncap update) throws IOException{
		NNUMkdirs mkdir=(NNUMkdirs)update.body;
		namesystem.dir.mkdirs(mkdir.src, mkdir.perm, false, mkdir.timestamp);
		namesystem.getEditLog().logSync();
	}

	private void updateLease(NNUpdateEncap update) throws IOException{
		NNUpdateLease lease=(NNUpdateLease)update.body;
		updateLeaseInternal(lease);
	}
	
	private void updateLeaseBatch(NNUpdateEncap update) throws IOException{
		NNULeaseBatch lb=(NNULeaseBatch)update.body;
		if(lb.lb==null||lb.lb.size()==0){
			LOG.warn("LeaseBatch: empty lease batch got");
			return;
		}
		for(Iterator<NNUpdateLease> iter=lb.lb.iterator();iter.hasNext();){
			NNUpdateLease u=iter.next();
			updateLeaseInternal(u);
		}
	}
	
	private void updateLeaseInternal(NNUpdateLease lease) throws IOException{
		if(lease.update){//renew lease
			namesystem.renewLease(lease.holder);
		}else{
			//rpy Lease l = namesystem.leaseManager.getLease(new StringBytesWritable(lease.holder));
			//rpy List<StringBytesWritable> removing = new ArrayList<StringBytesWritable>();
			//rpy StringBytesWritable[] leasePaths = new StringBytesWritable[l.getPaths().size()];
			//rpy l.getPaths().toArray(leasePaths);
			//rpy for(StringBytesWritable p : leasePaths) {
			Lease l = namesystem.leaseManager.getLease(lease.holder);
			List<String> removing = new ArrayList<String>();
			String[] leasePaths = new String[l.getPaths().size()];
			l.getPaths().toArray(leasePaths);			
		    for(String p : leasePaths) {
		        try {
		            //rpy namesystem.internalReleaseLease(l, p.getString());
		        	namesystem.internalReleaseLease(l, p);
		        } catch (IOException e) {
		            LOG.error("Cannot release the path "+p+" in the lease "+l, e);
		            removing.add(p);
		        }
		    }

		    //rpy for(StringBytesWritable p : removing) {
		    for(String p : removing) {
			    try {
			    	//rpy namesystem.leaseManager.removeLease(l, p.getString());
			    	namesystem.leaseManager.removeLease(l, p);
			    }catch (Exception e) { //rpy catch (IOException e) {
			    	LOG.error("Cannot removeLease: oldest=" + l + ", p=" + p, e);
		        }
		    }
		}
	}

	private void updateHeartbeatBatch(NNUpdateEncap update) throws IOException{
		NNUDnodeHBBatch hbb=(NNUDnodeHBBatch)update.body;
		if(hbb.hb==null||hbb.hb.size()==0){
			LOG.warn("HBBatch: empty heartbeat batch got");
			return;
		}
		LOG.debug("Heartbeat for "+hbb.hb.size()+" nodes");
		for(Iterator<NNUDnodeHB> iter=hbb.hb.iterator();iter.hasNext();){
			NNUDnodeHB u=iter.next();
			updateHeartbeatInternal(u);
		}
	}

	private void updateHeartbeat(NNUpdateEncap update) throws IOException{
		NNUDnodeHB hb=(NNUDnodeHB)update.body;
		updateHeartbeatInternal(hb);
	}

	private void updateHeartbeatInternal(NNUDnodeHB hb) throws IOException{
		
		
		Object xferResults[] = new Object[2];
	    xferResults[0] = xferResults[1] = null;
	    Object deleteList[] = new Object[1];
	    deleteList[0] = null;
		
	    namesystem.handleHeartbeat(hb.dnode, hb.capacity, hb.dfsUsed, hb.remaining, 
				hb.xceiverCount, 
				hb.xmitsInProgress);
	}

	private void updateDNReg(NNUpdateEncap update) throws IOException{
		NNUDnodeReg reg=(NNUDnodeReg)update.body;
		namesystem.registerDatanode(reg.dnodeReg);
	}
	
	private void updateDNBlkRep(NNUpdateEncap update) throws IOException{
		NNUBlockRep rep=(NNUBlockRep)update.body;
		BlockListAsLongs blist = new BlockListAsLongs(rep.blocks);
		namesystem.processReport(rep.dnode, blist);
	}
	
	private void updateBlockRecv(NNUpdateEncap update) throws IOException{
		NNUBlockRecv recv=(NNUBlockRecv)update.body;
		namesystem.blockReceived(recv.dnode, recv.blk, recv.delHint);
	}

	private void updateDNodeRm(NNUpdateEncap update) throws IOException{
		NNUDnodeRm dnode=(NNUDnodeRm)update.body;
		namesystem.removeDatanode(dnode.dnode);
	}
	
	private void updateReplicaMon(NNUpdateEncap update) throws IOException{
		namesystem.computeDatanodeWork();
		namesystem.processPendingReplications();
	}
	
	/*private String processUpdate(DataInputStream in, DataOutputStream out, NNUpdateInfo head) throws IOException{
		NNUpdateEncap update=new NNUpdateEncap();
		update.head=head;
		update.body=NNUpdateUnit.read(in, head.updateType);
		
		if(update.head.updateID.compareTo(this.slaveInfo.updateID) > 0){
			switch(update.head.updateType){
			case NNUpdateInfo.NNU_NEWFILE:
				LOG.debug("New file starting");
				updateNewFile(update);
				break;
			case NNUpdateInfo.NNU_INODE:
				LOG.debug("change path property");
				updateINode(update);
				break;
			case NNUpdateInfo.NNU_BLK:
				LOG.debug("change in block, add or remove");
				updateBlock(update);
				break;
			case NNUpdateInfo.NNU_CLSFILE:
				LOG.debug("File creating closed, abandon or complete");
				updateCloseFile(update);
				break;
			case NNUpdateInfo.NNU_MVRM:
				LOG.debug("Remove or Rename a file");
				updateMvRm(update);
				break;
			case NNUpdateInfo.NNU_MKDIR:
				LOG.debug("mkdirs");
				updateMkdirs(update);
				break;
			case NNUpdateInfo.NNU_LEASE:
				LOG.debug("renew lease");
				updateLease(update);
				break;
			case NNUpdateInfo.NNU_LEASE_BATCH:
				LOG.debug("renew many leases");
				updateLeaseBatch(update);
				break;
			case NNUpdateInfo.NNU_DNODEHB:
				LOG.debug("dnode heartbeat");
				updateHeartbeat(update);
				break;
			case NNUpdateInfo.NNU_DNODEHB_BATCH:
				LOG.debug("dnode heartbeat");
				updateHeartbeatBatch(update);
				break;
			case NNUpdateInfo.NNU_DNODEREG:
				LOG.debug("dnode register");
				updateDNReg(update);
				break;
			case NNUpdateInfo.NNU_DNODEBLK:
				LOG.debug("dnode report blocks");
				updateDNBlkRep(update);
				break;
			case NNUpdateInfo.NNU_BLKRECV:
				LOG.debug("dnode received block");
				updateBlockRecv(update);
				break;
			case NNUpdateInfo.NNU_DNODERM:
				LOG.debug("remove datanode");
				updateDNodeRm(update);
				break;
			case NNUpdateInfo.NNU_REPLICAMON:
				LOG.debug("do as replication monitor");
				updateReplicaMon(update);
				break;
			case NNUpdateInfo.NNU_NOP:
				break;
			default:
				LOG.warn("Unknown update type " + update.head.updateType);
				break;
			}
			
			LOG.debug("it is time to store the update");
			
			if(update.head.updateType != NNUpdateInfo.NNU_NOP)
				updateQueue.put(update.head.updateID, update);
			
			int queue_max=512;
			if(updateQueue.size()>queue_max){
				Set<String> keys=updateQueue.keySet();
				String halfQueue="";
				int counter=0;
				for(Iterator<String> iter=keys.iterator();iter.hasNext();){
					halfQueue=iter.next();
					if( ++counter > queue_max/2)
						break;
				}
				updateQueue=updateQueue.tailMap(halfQueue);
			}

		}else if(update.head.updateType == NNUpdateInfo.NNU_NOP){
			LOG.debug("nop received");
		}else{
			LOG.debug("old update received");
		}

		LOG.debug("sync processed, reply master");

		out.writeShort(DataTransferProtocol.OP_STATUS_SUCCESS);
		LOG.debug("start flushing the success message");
		out.flush();
		LOG.debug("completed flushing the success message");
		
		return update.head.updateID;
	}*/

		private String processUpdate(DataInputStream in, DataOutputStream out, NNUpdateInfo head) throws IOException{
		NNUpdateEncap update=new NNUpdateEncap();
		update.head=head;
		update.body=NNUpdateUnit.read(in, head.updateType);
		
		boolean result = true;
		
		try{
		
			if(update.head.updateID.compareTo(this.slaveInfo.updateID) > 0){
				switch(update.head.updateType){
				case NNUpdateInfo.NNU_NEWFILE:
					LOG.debug("New file starting");
					updateNewFile(update);
					break;
				case NNUpdateInfo.NNU_INODE:
					LOG.debug("change path property");
					updateINode(update);
					break;
				case NNUpdateInfo.NNU_BLK:
					LOG.debug("change in block, add or remove");
					updateBlock(update);
					break;
				case NNUpdateInfo.NNU_CLSFILE:
					LOG.debug("File creating closed, abandon or complete");
					updateCloseFile(update);
					break;
				case NNUpdateInfo.NNU_MVRM:
					LOG.debug("Remove or Rename a file");
					updateMvRm(update);
					break;
				case NNUpdateInfo.NNU_MKDIR:
					LOG.debug("mkdirs");
					updateMkdirs(update);
					break;
				case NNUpdateInfo.NNU_LEASE:
					LOG.debug("renew lease");
					updateLease(update);
					break;
				case NNUpdateInfo.NNU_LEASE_BATCH:
					LOG.debug("renew many leases");
					updateLeaseBatch(update);
					break;
				case NNUpdateInfo.NNU_DNODEHB:
					LOG.debug("dnode heartbeat");
					updateHeartbeat(update);
					break;
				case NNUpdateInfo.NNU_DNODEHB_BATCH:
					LOG.debug("dnode heartbeat");
					updateHeartbeatBatch(update);
					break;
				case NNUpdateInfo.NNU_DNODEREG:
					LOG.debug("dnode register");
					updateDNReg(update);
					break;
				case NNUpdateInfo.NNU_DNODEBLK:
					LOG.debug("dnode report blocks");
					updateDNBlkRep(update);
					break;
				case NNUpdateInfo.NNU_BLKRECV:
					LOG.debug("dnode received block");
					updateBlockRecv(update);
					break;
				case NNUpdateInfo.NNU_DNODERM:
					LOG.debug("remove datanode");
					updateDNodeRm(update);
					break;
				case NNUpdateInfo.NNU_REPLICAMON:
					LOG.debug("do as replication monitor");
					updateReplicaMon(update);
					break;
				case NNUpdateInfo.NNU_NOP:
					break;
				default:
					LOG.warn("Unknown update type " + update.head.updateType);
					break;
				}
				
				LOG.debug("it is time to store the update");
				
				if(update.head.updateType != NNUpdateInfo.NNU_NOP)
					updateQueue.put(update.head.updateID, update);
				
				int queue_max=512;
				if(updateQueue.size()>queue_max){
					Set<String> keys=updateQueue.keySet();
					String halfQueue="";
					int counter=0;
					for(Iterator<String> iter=keys.iterator();iter.hasNext();){
						halfQueue=iter.next();
						if( ++counter > queue_max/2)
							break;
					}
					updateQueue=updateQueue.tailMap(halfQueue);
				}
	
			}else if(update.head.updateType == NNUpdateInfo.NNU_NOP){
				LOG.debug("nop received");
			}else{
				LOG.debug("old update received");
			}
		
		} catch(IOException ioe) {
			LOG.warn("NN Sycn Slave processUpdate occured IOException. updateType: " + update.head.updateType + ", updateID: " + update.head.updateID, ioe);
			result = false;
		}

		LOG.debug("sync processed, reply master");

		if( result ) {
			out.writeShort(DataTransferProtocol.OP_STATUS_SUCCESS);
		}
		else {
			LOG.warn("NN Sycn Slave processUpdate write DataTransferProtocol.OP_STATUS_ERROR to Master NameNode.");
			out.writeShort(DataTransferProtocol.OP_STATUS_ERROR);
		}
	
		out.flush();
		
		return update.head.updateID;
	}
	
	private NNUpdateInfo readUpdateHead(DataInputStream in) throws IOException{
		NNUpdateInfo head=new NNUpdateInfo();
		head.readFields(in);
		
		//check the head here;
		
		return head;
	}
	
	private void recvOneUpdate(DataInputStream in, DataOutputStream out, NNUpdateInfo head) throws IOException{
		
		this.slaveInfo.setUpdateID(processUpdate(in,out,head));
		this.slaveInfo.setUpdateStatus(NNSyncer.NNUS_UPTODATE);
		
	}
	
	private void recvMassUpdate(DataInputStream in, DataOutputStream out, NNUpdateInfo head) throws IOException{
		
		while(true){
			String updateID=processUpdate(in,out,head);
			LOG.info("Mass Update to updateID "+updateID);
			this.slaveInfo.setUpdateID(updateID);
			if(updateID.equals("")){
				LOG.debug("mass update finished.");
				break;
			}
		}
		
		this.slaveInfo.setUpdateStatus(NNSyncer.NNUS_UPTODATE);		
		nnkeeper.addNameNode( namenode.getNameNodeAddress().getHostName() + ":" + namenode.getNameNodeAddress().getPort() );
	}
	
	private void recvWholeWorld(DataInputStream in, DataOutputStream out, NNUpdateInfo head) throws IOException{
		
		LOG.info("receive whole world started");
		
//		NNUpdateInfo head=new NNUpdateInfo();
//		head.readFields(in);
//		if(head.updateType!=NNUpdateInfo.NNU_WORLD){
//			LOG.error("update should be but is not NNU_WORLD!");
//			throw new IOException("update should be NNU_WORLD");
//		}
//		LOG.debug("a head of NNU_WORLD is recieved");
		NNUpdateWorld world=new NNUpdateWorld(namesystem.dir,namesystem.datanodeMap,namesystem.blocksMap);
		world.readFields(in);
		
		//save FSImage files
		namesystem.dir.fsImage.saveFSImage();
		
		LOG.debug("now, turn on the dir (orig: "+namesystem.dir.ready+")");
		namesystem.dir.ready=true;
		
		out.writeShort(DataTransferProtocol.OP_STATUS_SUCCESS);
		
		out.flush();
		
		LOG.info("receive whole world ended");
				
		this.slaveInfo.setUpdateID(head.updateID);
		this.slaveInfo.setUpdateStatus(NNSyncer.NNUS_UNSYNCED);
	}
		
}
