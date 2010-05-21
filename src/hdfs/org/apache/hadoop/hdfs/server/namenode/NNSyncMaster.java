package org.apache.hadoop.hdfs.server.namenode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.*;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;

public class NNSyncMaster extends NNSyncer implements NNCProtocol {

	private Server server; //as master, whenever a slave registered, it push sync data and other cmd via the established socket
	
	Map<String,NNSlaveInfo> uptodateSlaves = new HashMap<String,NNSlaveInfo>();
	Map<String,NNSlaveInfo> syncedSlaves = new HashMap<String,NNSlaveInfo>();
	Map<String,NNSlaveInfo> unsyncedSlaves = new HashMap<String,NNSlaveInfo>();
	Map<String,NNSlaveInfo> emptySlaves = new HashMap<String,NNSlaveInfo>();
	
	Integer slaveStatusLock = 0;
	
//	Integer uptodateSlavesLock=0;
//	Integer syncedSlavesLock=0;
//	Integer unsyncedSlavesLock=0;
//	Integer emptySlavesLock=0;
	
	Integer syncFlag = 0;
	Integer syncCount = 0;
	
	//volatile String firstUpdateInQueue;
	//volatile String oldestUpdateOfSlaves;
	volatile String latestUpdate;
	
	//private ReentrantReadWriteLock rwlWorld;
	//boolean isDumpingWorld=false;
	//boolean isUpdating=false;
	
	NNUpdateEncap fsImageDump;

	public NNSyncMaster(NameNode nn, FSNamesystem namesystem, Configuration conf) throws IOException{
		super(nn,namesystem, conf);
		updateQueue=new TreeMap<String, NNUpdateEncap>();
		initialize(conf);
    }
	
	public NNSyncMaster(NNSyncSlave slave, Configuration conf) throws IOException{
		super(slave.namenode,slave.namesystem,conf);
		updateQueue=slave.updateQueue;
		slave.updateQueue=null;
		initialize(conf);
	}
	
	private void initialize(Configuration conf) throws IOException{
		//rwlWorld=new ReentrantReadWriteLock();
		
        String machineName = DNS.getDefaultHost(
                conf.get("dfs.nnsyncslave.dns.interface","default"),
                conf.get("dfs.nnsyncslave.dns.nameserver","default"));
        
        int syncMasterPort=conf.getInt("dfs.nnc.masterport", 50078);
        syncMasterAddress=NetUtils.createSocketAddr(machineName+':'+syncMasterPort);
        
	    LOG.info("NNSyncMaster start at: " + syncMasterAddress);
	    server = RPC.getServer(this, syncMasterAddress.getHostName(), syncMasterAddress.getPort(),
	                                5, false, conf);
	    syncMasterAddress=server.getListenerAddress();
	    
	    socketWriteTimeout = conf.getInt("dfs.socket.write.timeout", HdfsConstants.WRITE_TIMEOUT);
	    socketTimeout =  conf.getInt("dfs.socket.timeout", HdfsConstants.READ_TIMEOUT);
	    
	    heartbeatBatchInterval=conf.getInt("heartbeat.recheck.interval", 5*60*1000)/10;
	    
	    latestUpdate=updateQueue.isEmpty()?"":updateQueue.lastKey();
	    //oldestUpdateOfSlaves="";
	    //firstUpdateInQueue="";

		server.start();

		String nnDescript = machineName + ":" + namenode.getNameNodeAddress().getPort();
		nnkeeper.setMasterNameNode(nnDescript);
		nnkeeper.addNameNode(nnDescript);
	}
	
	void slaveTransit(NNSlaveInfo orig, Map<String,NNSlaveInfo> origList, Map<String,NNSlaveInfo> targetList, int targetStat){
		
		synchronized(slaveStatusLock){
			
			if(orig == null){
				for(Iterator<String> iter=origList.keySet().iterator();iter.hasNext();){
					String slaveID=iter.next();
					NNSlaveInfo slave=origList.get(slaveID);
					iter.remove();
					slave.setUpdateStatus(targetStat);
					targetList.put(slaveID, slave);
				}
			}else{
				origList.remove(orig.getNNSlaveID());
				orig.setUpdateStatus(targetStat);
				targetList.put(orig.getNNSlaveID(), orig);
			}	
		}
		
	}
	
	Map<DatanodeID,NNUDnodeHB> heartbeatUpdate=new HashMap<DatanodeID,NNUDnodeHB>();
	long lastHeartbeatBatch=0;
	long heartbeatBatchInterval;
	Map<String,NNUpdateLease> leaseUpdate=new HashMap<String, NNUpdateLease>();
	long lastLeaseBatch=0;
	long leaseBatchInterval=FSConstants.LEASE_SOFTLIMIT_PERIOD;
	
	void onLowPriorityUpdate(int type, NNUpdateUnit u){
		switch(type){
		case NNUpdateInfo.NNU_DNODEHB:
			NNUDnodeHB dnhb=(NNUDnodeHB)u;
			synchronized(heartbeatUpdate){
				heartbeatUpdate.put(dnhb.dnode, dnhb);
				long timestamp=System.currentTimeMillis();
				if(lastHeartbeatBatch==0||timestamp-lastHeartbeatBatch>=heartbeatBatchInterval){
					NNUDnodeHBBatch hbb=new NNUDnodeHBBatch();
					hbb.hb=heartbeatUpdate.values();
					this.onUpdate(NNUpdateInfo.NNU_DNODEHB_BATCH, hbb);

					heartbeatUpdate=new HashMap<DatanodeID,NNUDnodeHB>();
					lastHeartbeatBatch=timestamp;
				}
			}
			break;
		case NNUpdateInfo.NNU_LEASE:
			NNUpdateLease renewLease=(NNUpdateLease)u;
			synchronized(leaseUpdate){
				leaseUpdate.put(renewLease.holder, renewLease);
				long timestamp=System.currentTimeMillis();
				if(timestamp-lastLeaseBatch >= leaseBatchInterval || lastLeaseBatch==0){
					NNULeaseBatch lb=new NNULeaseBatch();
					lb.lb=leaseUpdate.values();
					this.onUpdate(NNUpdateInfo.NNU_LEASE_BATCH, lb);
					
					leaseUpdate=new HashMap<String, NNUpdateLease>();
					lastLeaseBatch=timestamp;
				}
			}
			break;
		}
	}

	synchronized void onUpdate(int type, NNUpdateUnit u){
		NNUpdateEncap update = null;
		synchronized(syncFlag){
			while(syncFlag>0){
				LOG.info("last updating is running, lets wait it: "+syncFlag);
				try{
					syncFlag.wait();
				}catch(InterruptedException ignore){
				}
			}
			if(uptodateSlaves.isEmpty()){
				LOG.warn("no sync mode slaves...");
				syncFlag.notifyAll();
				return;
			}
			syncFlag++;
		}
		try{
			update=new NNUpdateEncap(new NNUpdateInfo(type),u);
			updateQueue.put(update.head.updateID, update);
			latestUpdate=update.head.updateID;
			LOG.debug("arriving new update "+update.head.updateID + " with typeid: "+type);
			
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

			slaveTransit(null,uptodateSlaves,syncedSlaves,NNSyncer.NNUS_SYNCED);
	                LOG.debug("begin sync "+update.head.updateID);
		}
		catch(Throwable e){
			update = null;
			synchronized(syncFlag){
				syncFlag.notifyAll();
				if(syncFlag>0){
					syncFlag--;
				}
			}
			LOG.warn("nnc, exception in onUpdate()");
		}
		
		if( update == null ){
			return;
		}


		Thread t=new Thread(new SyncMonitor(update));
		t.start();

		//LOG.info("updateing slave stats");
		LOG.debug("slave stats have been updated to uptodate");
	}
	/*
	 * Interface Methods
	 * @see org.apache.hadoop.ipc.VersionedProtocol#getProtocolVersion(java.lang.String, long)
	 */
	public long getProtocolVersion(String protocol, 
            long clientVersion) throws IOException { 
		LOG.info("slave acquire version...");
      if (protocol.equals(NNCProtocol.class.getName())) {
          return NNCProtocol.versionID; 
      } else {
          throw new IOException("Unknown protocol to nnc node: " + protocol);
      }
    }
	
	public synchronized NNSlaveInfo register(NNSlaveInfo slave) throws IOException{
		String slaveUpdateID=slave.getUpdateID();

		LOG.info("slave registering... slave{ " + slave.name + " || " + slave.getNNSlaveID());
		Map<String,NNSlaveInfo> initList=new HashMap<String,NNSlaveInfo>();
		initList.put(slave.getNNSlaveID(),slave);
		
		removeSlave(slave);
		
		if(slaveUpdateID==null || !this.updateQueue.containsKey(slaveUpdateID)){
			if( slaveUpdateID == null ){
				LOG.info("slave registered as empty, slaveUpdateID is null.");
			}
			else{
				LOG.info("slave registered as empty, updateQueue.containsKey(slaveUpdateID) is false.");
			}
			
			slaveTransit(null,initList,emptySlaves,NNSyncer.NNUS_EMPTY);
			Thread t=new Thread(new InjectToSlave(this,slave));
			this.shouldRecordUpdate=true;
			t.start();
		}else{
			LOG.info("slave registered as unsyned...");
			slaveTransit(null,initList,unsyncedSlaves,NNSyncer.NNUS_UNSYNCED);
			Thread t=new Thread(new SyncMass(this,slave,slaveUpdateID));
			t.start();
		}
		return slave;
	}
	
	private void removeSlave(NNSlaveInfo slave){
		
		LOG.info("nnc, now removeSlave current slave (" + slave.name + " slaveID is " + slave.getNNSlaveID() + ") from 4 HashMaps... ...");
		
		synchronized(slaveStatusLock){
			
			//String slaveID=slave.getNNSlaveID();
			
			for(Iterator<String> iter=uptodateSlaves.keySet().iterator();iter.hasNext();){
				String sID =iter.next();
				NNSlaveInfo slaveinfo=uptodateSlaves.get(sID);
				LOG.info("nnc, check uptodateSlaves key: " + slaveinfo.name + " || " + slaveinfo.getNNSlaveID());
				if( slaveinfo.name.equals(slave.name)){
					if( slaveinfo.in != null ){
						try{ if(slave.in != null) slave.in.close(); } catch(IOException iex) {}
						slaveinfo.in = null;
					}
					if( slaveinfo.out != null ){
						try{ if(slave.out != null) slave.out.close(); } catch(IOException oex) {}
						slaveinfo.out = null;
					}
					iter.remove();
					LOG.info("nnc, now removeSlave current slave (" + slave.name + " slaveID is " + slave.getNNSlaveID() + ") from uptodateSlaves OK");
				}
			}
			
			for(Iterator<String> iter=syncedSlaves.keySet().iterator();iter.hasNext();){
				String sID =iter.next();
				NNSlaveInfo slaveinfo=syncedSlaves.get(sID);
				LOG.info("nnc, check syncedSlaves key: " + slaveinfo.name + " || " + slaveinfo.getNNSlaveID());
				if( slaveinfo.name.equals(slave.name)){
					if( slaveinfo.in != null ){
						try{ if(slave.in != null) slave.in.close(); } catch(IOException iex) {}
						slaveinfo.in = null;
					}
					if( slaveinfo.out != null ){
						try{ if(slave.out != null) slave.out.close(); } catch(IOException oex) {}
						slaveinfo.out = null;
					}
					iter.remove();
					LOG.info("nnc, now removeSlave current slave (" + slave.name + " slaveID is " + slave.getNNSlaveID() + ") from syncedSlaves OK");
				}
			}
			
			for(Iterator<String> iter=unsyncedSlaves.keySet().iterator();iter.hasNext();){
				String sID =iter.next();
				NNSlaveInfo slaveinfo=unsyncedSlaves.get(sID);
				LOG.info("nnc, check unsyncedSlaves key: " + slaveinfo.name + " || " + slaveinfo.getNNSlaveID());
				if( slaveinfo.name.equals(slave.name)){
					if( slaveinfo.in != null ){
						try{ if(slave.in != null) slave.in.close(); } catch(IOException iex) {}
						slaveinfo.in = null;
					}
					if( slaveinfo.out != null ){
						try{ if(slave.out != null) slave.out.close(); } catch(IOException oex) {}
						slaveinfo.out = null;
					}
					iter.remove();
					LOG.info("nnc, now removeSlave current slave (" + slave.name + " slaveID is " + slave.getNNSlaveID() + ") from unsyncedSlaves OK");
				}
			}
			
			for(Iterator<String> iter=emptySlaves.keySet().iterator();iter.hasNext();){
				String sID =iter.next();
				NNSlaveInfo slaveinfo=emptySlaves.get(sID);
				LOG.info("nnc, check unsyncedSlaves key: " + slaveinfo.name + " || " + slaveinfo.getNNSlaveID());
				if( slaveinfo.name.equals(slave.name)){
					if( slaveinfo.in != null ){
						try{ if(slave.in != null) slave.in.close(); } catch(IOException iex) {}
						slaveinfo.in = null;
					}
					if( slaveinfo.out != null ){
						try{ if(slave.out != null) slave.out.close();} catch(IOException oex) {}
						slaveinfo.out = null;
					}
					iter.remove();
					LOG.info("nnc, now removeSlave current slave (" + slave.name + " slaveID is " + slave.getNNSlaveID() + ") from emptySlaves OK");
				}
			}
		}
	}
	
	public NamespaceInfo versionRequest() throws IOException{
		return new NamespaceInfo(namenode.namesystem.dir.fsImage.getNamespaceID(),
				namenode.namesystem.dir.fsImage.getCTime(),
				namenode.namesystem.getDistributedUpgradeVersion());
	}
	
	/*
	 * Inner classes for sync to slaves
	 */
	
	class SyncMonitor implements Runnable{
		private int daemonCnt=0;
		NNUpdateEncap update;
		Integer refLock=0;
		
		public SyncMonitor(NNUpdateEncap u){
			this.update=u;
		}
		
		public void increase(boolean inc){
			synchronized(refLock){
				if(inc){
					++daemonCnt;
					LOG.debug("refcount++: "+daemonCnt);
				}else{
					if(daemonCnt>0)
						--daemonCnt;
					
					if(daemonCnt==0)
						refLock.notifyAll();
					
					LOG.debug("refcount--: "+daemonCnt);
				}
			}
		}
		
		class SendOne implements Runnable{
			
			SyncMonitor parent;
			NNSlaveInfo slave;
			
			public SendOne(SyncMonitor parent, NNSlaveInfo slave){
				//if(slave.getUpdateStatus()!=NNSyncer.NNUS_SYNCED)
				//	throw new IOException();
				this.parent=parent;
				this.slave=slave;
			}
			
			public void run(){
				
				DataOutputStream out=slave.out;
				DataInputStream in=slave.in;
				Socket target = null;
				try{
					//transfer update					
					if(out==null || in==null ){
						InetSocketAddress targetAddr=NetUtils.createSocketAddr(slave.getName());
						target= (socketWriteTimeout > 0) ? SocketChannel.open().socket() : new Socket();
						target.connect(targetAddr, socketTimeout);
						target.setSoTimeout(10 * socketTimeout);
						
				        long writeTimeout = socketWriteTimeout + HdfsConstants.WRITE_TIMEOUT_EXTENSION * 10;

				        out = new DataOutputStream(new BufferedOutputStream(NetUtils.getOutputStream(target, writeTimeout), FSConstants.SMALL_BUFFER_SIZE));
						in = new DataInputStream(new BufferedInputStream(
				        		NetUtils.getInputStream(target), FSConstants.BUFFER_SIZE));
						
						slave.in=in;
						slave.out=out;
						
						LOG.info("nnc, in SendOne, create a new io stream for sync a slave, L->R " + target.getLocalSocketAddress().toString()+" -> " + target.getRemoteSocketAddress().toString());
					}
					
					//LOG.info("nnc, write the update id: " + update.head.updateID + " to " + slave.getName() + " || " + slave.getNNSlaveID() + " ...");
			        update.write(out);
			        out.flush();
			        LOG.debug("nnc, write the update id: " + update.head.updateID + " to " + slave.getName() + " || " + slave.getNNSlaveID() + " done");
			        
                                    if (in.readShort() == DataTransferProtocol.OP_STATUS_SUCCESS) {
				        LOG.debug("sync update finished, signal received. slave NameNode Sync the update OK.");
			            }else{
			                LOG.warn("Master write the update id: " + update.head.updateID + " to " + slave.getName() + " || " + slave.getNNSlaveID() + "update type:" + update.head.updateType + ", but slave NameNode Sync the update error.");
                                    
			                throw new IOException("slave NameNode occured some error.");
			            }
						
				}catch(IOException e){
					if( slave.in != null ){
						try{slave.in.close(); } catch(IOException iex) {}
					}
					if( slave.out != null ){
						try{slave.out.close();} catch(IOException oex) {}
					}

					slave.in=null;
					slave.out=null;
					
					if( target != null ){
						try {target.shutdownOutput();} catch(Exception ex1) {}
						try {target.close();} catch(Exception ex2) {}
					}
					
					LOG.warn("Exception occured when sync update " + update.head.updateID + " to " + slave.getName() + " || " + slave.getNNSlaveID() + ", now remove the slave from 4 HashMaps.");
                                        removeSlave(slave);
				}catch(Throwable t){
					if( slave.in != null ){
						try{ slave.in.close(); } catch(IOException iex) {}
					}
					if( slave.out != null ){
						try{slave.out.close();} catch(IOException oex) {}
					}

					slave.in=null;
					slave.out=null;
					
					if( target != null ){
						try {target.shutdownOutput();} catch(Exception ex1) {}
						try {target.close();} catch(Exception ex2) {}
					}
					
					LOG.warn("unexpected exception in senting update to a slave" + t + ", now remove the slave from 4 HashMaps.");
                                        removeSlave(slave);
					
				}finally{
					parent.increase(false);
				}
			}
			
		}
		
		public void run(){
			try{
				//start threads
				LOG.debug("begin to sync slaves "+syncedSlaves.size());
				
				for(Iterator<String> iter=syncedSlaves.keySet().iterator();iter.hasNext();){
					String slaveID=iter.next();
					NNSlaveInfo slave=syncedSlaves.get(slaveID);
					LOG.debug("nnc, SyncMonitor sync to slave: " + slave.name + " || " + slave.getNNSlaveID());
					Thread t=new Thread(new SendOne(this,slave));
					increase(true);
					t.start();
					LOG.debug("nnc, sync thread started.");
				}
				//wait daemons completed
				//synchronized(refCount){
				synchronized(refLock){
					while(this.daemonCnt>0){
						try{
							refLock.wait();
						}catch(InterruptedException e){
							//ignore
						}
					}
				}
					
				slaveTransit(null,syncedSlaves,uptodateSlaves,NNSyncer.NNUS_UPTODATE);

			}catch(Throwable t){
				LOG.warn("exception while start or waiting syncs"+t);
			}finally{
				synchronized(syncFlag){
					syncFlag.notifyAll();
					if(syncFlag>0){
						syncFlag--;
						LOG.debug("nnc, syncFlag-- in SyncMonitor.run(), after, syncFlag is " + syncFlag);
					}
				}
				LOG.debug("nnc, a synchronization existed.");
			}
		}
	}

	static class SyncMass implements Runnable{
		NNSyncMaster master;
		NNSlaveInfo slave;
		String updateID;
		
		public SyncMass(NNSyncMaster master,NNSlaveInfo s, String updateID){
			this.master=master;
			this.slave=s;
			this.updateID=updateID;
		}
		
		public void run(){
			try{
				DataOutputStream out = slave.out;
				DataInputStream in=slave.in;
				
				if(out==null || in==null ){
					InetSocketAddress targetAddr=NetUtils.createSocketAddr(slave.getName());
					Socket target= (master.socketWriteTimeout > 0) ? SocketChannel.open().socket() : new Socket();
					target.connect(targetAddr, master.socketTimeout);
					target.setTcpNoDelay(true);
					target.setSoTimeout(10 * master.socketTimeout);
				
					long writeTimeout = master.socketWriteTimeout + HdfsConstants.WRITE_TIMEOUT_EXTENSION * 10;
	
					out = new DataOutputStream(new BufferedOutputStream(NetUtils.getOutputStream(target, writeTimeout), FSConstants.SMALL_BUFFER_SIZE));
					in=new DataInputStream(new BufferedInputStream(
							NetUtils.getInputStream(target), FSConstants.BUFFER_SIZE));
					
					slave.in = in;
					slave.out = out;
					
					LOG.info("nnc, in SyncMass, create a new io stream for sync a slave, L->R " + target.getLocalSocketAddress().toString()+" -> " + target.getRemoteSocketAddress().toString());
				}
				
				LOG.info("nnc, update from "+updateID);
				
				if(master.updateQueue.isEmpty()){
					LOG.info("nnc, update queue of muster is empty....");
				}else{
					LOG.info("nnc, the latest key in update queue is "+master.updateQueue.lastKey());
				}
				
				while(true){
					synchronized(master.syncFlag){
						while(master.syncFlag>0){
							LOG.info("nnc, sync is running, lets wait it: "+master.syncFlag);
							try{
								master.syncFlag.wait();
							}catch(InterruptedException ignore){
							}
						}
						master.syncFlag++;
					}
					
					if(master.updateQueue.isEmpty() || updateID.equals(master.updateQueue.lastKey())){
						LOG.info("nnc, syncMass now locked by transiting.....");
						break;
					}
					
					synchronized(master.syncFlag){
						if(master.syncFlag>0){
							master.syncFlag--;
						}
						master.syncFlag.notifyAll();
					}
					
					if(updateID.equals("")){
						updateID=master.updateQueue.firstKey();
						LOG.info("nnc, update from " + updateID);
					}
						
					SortedMap<String,NNUpdateEncap> updateMap=master.updateQueue.tailMap(updateID);
					for(Iterator<NNUpdateEncap> iter=updateMap.values().iterator();iter.hasNext();){
						NNUpdateEncap update=iter.next();
						LOG.info("update "+update.head.updateID+" is being sent.");
						update.head.updateFlag=NNUpdateInfo.NNF_MASS;
						update.write(out);
						out.flush();
						LOG.info("nnc, update "+update.head.updateID + " has been sent.");
						if (in.readShort() == DataTransferProtocol.OP_STATUS_SUCCESS) {
						    LOG.debug("nnc, sync update finished, signal received.");
                                                }else{
			            	            LOG.warn("nnc, SyncMass finished, slave NameNode occured some error.");
			            	            throw new IOException("slave NameNode occured some error.");
			                        }
					}
					updateID=updateMap.lastKey();
					LOG.info("nnc, update to... " + updateID);
				}
				LOG.info("nnc, syncMass synced to " + updateID);
				
				NNUpdateEncap eof=new NNUpdateEncap();
				eof.head.updateID="";
				eof.head.updateFlag=NNUpdateInfo.NNF_MASS;
				eof.head.updateType=NNUpdateInfo.NNU_NOP;
				eof.write(out);
				
				out.flush();
					
				try{
                                    if (in.readShort() == DataTransferProtocol.OP_STATUS_SUCCESS) {
                                        LOG.debug("nnc, sync update finished, signal received.");
                                    } else {
                                        LOG.warn("nnc, sync finished, slave NameNode occured some error.");
                                        throw new IOException();
		                    }
				}catch(ClosedChannelException e){
                                    //LOG.warn("nnc, wait but cannot got anything",e);
                                    throw new IOException(e.toString());
				}
				
				//IOUtils.closeStream(out);
				//IOUtils.closeStream(in);
				
				//target.close();
				
			    //LOG.info("nnc, syncMass target closed");
				
				master.slaveTransit(slave,master.unsyncedSlaves,master.uptodateSlaves,NNSyncer.NNUS_UPTODATE);
			    
				LOG.debug("nnc, adjusted the status of slave");
			    
			}catch(IOException e){
				LOG.warn("nnc, SyncMass met exceptions " + e.getClass().getName() + " with message: "+ e.toString());
				if( slave.in != null ){
					try{ slave.in.close(); } catch(IOException iex) {}
				}
				if( slave.out != null ){
					try{slave.out.close();} catch(IOException oex) {}
				}

				slave.in=null;
				slave.out=null;
				master.removeSlave(slave);
			}finally{
				synchronized(master.syncFlag){
					master.syncFlag.notifyAll();
					if(master.syncFlag>0){
						master.syncFlag--;
					}
						
				}
			}
		}
	}
	static class InjectToSlave implements Runnable{
		NNSlaveInfo slave;
		NNSyncMaster master;
		
		InjectToSlave(NNSyncMaster master, NNSlaveInfo slave){
			this.master=master;
			this.slave=slave;
		}
		
		public void run(){
			//(master.fsImageDump==null){
			master.fsImageDump=new NNUpdateEncap();
		
			String slaveUpdateID;
			NNUpdateWorld world;
			//TODO: should I process the timestamp?
			//master.fsImageDump.head.timeStamp=;

			synchronized(master.syncFlag){
				while(master.syncFlag>0){
					LOG.info("sync is running, lets wait it: "+master.syncFlag);
					try{
						master.syncFlag.wait();
					}catch(InterruptedException ignore){
					}
				}
				master.syncFlag++;
			
				try{
					LOG.info("nnc, begin dumping the world");
					slaveUpdateID = master.latestUpdate;
					master.fsImageDump.head.updateID = slaveUpdateID;
					master.fsImageDump.head.updateType = NNUpdateInfo.NNU_WORLD;
					master.fsImageDump.head.updateFlag = NNUpdateInfo.NNF_WORLD;
					world = new NNUpdateWorld(master.namenode.namesystem.dir,master.namenode.namesystem.datanodeMap,master.namenode.namesystem.blocksMap);
					world.dumpWorld();
					LOG.info("nnc, dumping the world finished");
				}finally{
						if(master.syncFlag>0){
							master.syncFlag--;
						}	
						master.syncFlag.notifyAll();
				}
			}
			//master.fsImageDump.body=world;
			
			try{
				InetSocketAddress targetAddr=NetUtils.createSocketAddr(slave.getName());
				Socket target= (master.socketWriteTimeout > 0) ? SocketChannel.open().socket() : new Socket();
				LOG.info("nnc, try to connect the slave "+slave.getName());
				target.connect(targetAddr, master.socketTimeout);
				target.setTcpNoDelay(true);
				target.setSoTimeout(10 * master.socketTimeout);
			
				long writeTimeout = master.socketWriteTimeout + HdfsConstants.WRITE_TIMEOUT_EXTENSION * 10;

				DataOutputStream out = new DataOutputStream(new BufferedOutputStream(NetUtils.getOutputStream(target, writeTimeout), FSConstants.SMALL_BUFFER_SIZE));
				DataInputStream in=new DataInputStream(new BufferedInputStream(
		        		NetUtils.getInputStream(target), FSConstants.BUFFER_SIZE));
				
                                LOG.info("nnc, in InjectToSlave, create a new io stream for sync a slave, L->R " + target.getLocalSocketAddress().toString()+" -> " + target.getRemoteSocketAddress().toString());

				LOG.info("nnc, start to inject the slave");

				master.fsImageDump.write(out);
				world.write(out);
				out.flush();

				LOG.info("nnc, inject finished, wait signal...");
                                //try{
                                    if (in.readShort() == DataTransferProtocol.OP_STATUS_SUCCESS) {
                                        LOG.info("nnc, inject finished, signal received.");
                                    }else{
                                        LOG.warn("nnc, inject finished, slave NameNode occured some error.");
                                        throw new IOException("slave NameNode occured some error.");
                                    }
                                //} catch (IOException ioe) {}
		          
		                slave.in=in;
		                slave.out=out;
		        
		                //IOUtils.closeStream(out);
				//IOUtils.closeStream(in);

				//target.close();
				
				master.slaveTransit(slave, master.emptySlaves, master.unsyncedSlaves, NNSyncer.NNUS_UNSYNCED);
				
				LOG.info("nnc, current injectiong sync to updateID "+ slaveUpdateID);
				
				Thread t=new Thread(new SyncMass(master,slave,slaveUpdateID));
				t.start();

			}catch(IOException e){
				if( slave.in != null ){
					try{ slave.in.close(); } catch(IOException iex) {}
				}
				if( slave.out != null ){
					try{slave.out.close();} catch(IOException oex) {}
				}

				slave.in=null;
				slave.out=null;
				
				master.removeSlave(slave);  //rpy
				
			}

		}


	}
	
}
