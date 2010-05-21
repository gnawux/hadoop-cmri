package org.apache.hadoop.hdfs.server.namenode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.*;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.*;

public abstract class NNUpdateUnit implements Writable {
	public static final Log LOG = LogFactory.getLog("org.apache.hadoop.dfs.NNUpdateUnit");
	
	public static NNUpdateUnit read(DataInput in, int updateType) throws IOException{
		NNUpdateUnit body=null;
		
		switch(updateType){
		case NNUpdateInfo.NNU_NEWFILE:
			body=new NNUStartFile();
			body.readFields(in);
			break;
		case NNUpdateInfo.NNU_INODE:
			body=new NNUINodeInfo();
			body.readFields(in);
			break;
		case NNUpdateInfo.NNU_BLK:
			body=new NNUpdateBlk();
			body.readFields(in);
			break;
		case NNUpdateInfo.NNU_CLSFILE:
			body=new NNUCloseNewFile();
			body.readFields(in);
			break;
		case NNUpdateInfo.NNU_MVRM:
			body=new NNUMvRm();
			body.readFields(in);
			break;
		case NNUpdateInfo.NNU_MKDIR:
			body=new NNUMkdirs();
			body.readFields(in);
			break;
		case NNUpdateInfo.NNU_LEASE:
			body=new NNUpdateLease();
			body.readFields(in);
			break;
		case NNUpdateInfo.NNU_DNODEHB:
			body=new NNUDnodeHB();
			body.readFields(in);
			break;
		case NNUpdateInfo.NNU_DNODEHB_BATCH:
			body=new NNUDnodeHBBatch();
			body.readFields(in);
			break;
		case NNUpdateInfo.NNU_DNODEREG:
			body=new NNUDnodeReg();
			body.readFields(in);
			break;
		case NNUpdateInfo.NNU_DNODEBLK:
			body=new NNUBlockRep();
			body.readFields(in);
			break;
		case NNUpdateInfo.NNU_BLKRECV:
			body=new NNUBlockRecv();
			body.readFields(in);
			break;
		case NNUpdateInfo.NNU_DNODERM:
			body=new NNUDnodeRm();
			body.readFields(in);
			break;
		case NNUpdateInfo.NNU_REPLICAMON:
			//body=new 
			//body.readFields(in);
			break;
		case NNUpdateInfo.NNU_NOP:
			break;
		case NNUpdateInfo.NNU_WORLD:
			break;
		default:
			break;
		}

		return body;
	}
}

/*
 * A new block added or remove in/from blocksMap and datanodeMap 
 */
class NNUpdateBlk extends NNUpdateUnit{
	Block blk;
	String src; 
	String holder;
	boolean add;
	LinkedList<String> dnodes;
	
	public void readFields(DataInput in) throws IOException {
		blk=new Block();
		blk.readFields(in);
		src=Text.readString(in);
		holder=Text.readString(in);
		add=in.readBoolean();
		int dnodeCount=in.readInt();
		if(dnodeCount > 0){
			dnodes=new LinkedList<String>();
			for(int i=0;i<dnodeCount;++i){
				dnodes.add(Text.readString(in));
			}
		} else {
			dnodes=null;
		}
	}
	
	public void write(DataOutput out) throws IOException{
		blk.write(out);
		Text.writeString(out, src);
		Text.writeString(out, holder);
		out.writeBoolean(add);
		int dnodeCount= (dnodes!=null)?dnodes.size():0;
		out.writeInt(dnodeCount);
		if(dnodeCount>0){
			for(ListIterator<String> iter=dnodes.listIterator();iter.hasNext();){
				Text.writeString(out, iter.next());
			}

		}
	}
}

class NNUBlockRecv extends NNUpdateUnit{
	DatanodeID dnode;
	Block blk;
	String delHint;
	public void readFields(DataInput in) throws IOException {
		dnode=new DatanodeID();
		blk=new Block();
		dnode.readFields(in);
		blk.readFields(in);
		delHint=Text.readString(in);
	}
	public void write(DataOutput out) throws IOException{
		DatanodeID d=new DatanodeID(dnode.getName(),dnode.getStorageID(),dnode.getInfoPort(),dnode.getIpcPort());
		d.write(out);
		blk.write(out);
		Text.writeString(out, delHint);
	}
}
/*
 * A new datanode registered with blocks or vanished 
 */
class NNUBlockRep extends NNUpdateUnit{
	DatanodeID dnode;
	long[] blocks;
	@Override
	public void readFields(DataInput in) throws IOException {
		int blkCount=in.readInt();
		LOG.info("blockCount: "+ blkCount);
		if(blkCount>=0){
			blocks=new long[blkCount];
			for(int i=0;i<blkCount;++i){
				blocks[i]=in.readLong();
			}
		}
		dnode=new DatanodeID();
		dnode.readFields(in);
//		LOG.info("dnode read "+dnode.getStorageID()+" Name: "+dnode.getName()+" info port: "+dnode.getInfoPort());
	}
	@Override
	public void write(DataOutput out) throws IOException {
		int blkCount=(blocks!=null)?blocks.length:0;
		LOG.info("length of block array: "+blkCount);
		out.writeInt(blkCount);
		if(blkCount>0){
			for(int i=0;i<blkCount;i++){
				out.writeLong(blocks[i]);
			}
		}
		DatanodeID d=new DatanodeID(dnode.getName(),dnode.getStorageID(),dnode.getInfoPort(),dnode.getIpcPort());
		d.write(out);
//		dnode.write(out);
//		LOG.info("dnode written "+dnode.getStorageID()+" Name: "+dnode.getName()+" info port: "+dnode.getInfoPort());
	}
}

class NNUMkdirs extends NNUpdateUnit{
	String	src;
	long	timestamp;
	PermissionStatus perm;
	
	NNUMkdirs(){}
	NNUMkdirs(String src, long timestamp, PermissionStatus perm){
		this.src=src;
		this.timestamp=timestamp;
		this.perm=new PermissionStatus(perm.getUserName(),(perm.getGroupName()==null)?"":perm.getGroupName(),perm.getPermission());
	}
	
	public void readFields(DataInput in) throws IOException{
		src=Text.readString(in);
		timestamp=in.readLong();
		PermissionStatus p=PermissionStatus.read(in);
		perm=new PermissionStatus(p.getUserName(),p.getGroupName(),p.getPermission());
		
		if(perm.getGroupName().equals("")){
			perm=new PermissionStatus(perm.getUserName(), null, perm.getPermission());
		}
	}
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, src);
		out.writeLong(timestamp);
		perm.write(out);
	}
}

class NNUStartFile extends NNUMkdirs{
	short	replication;
	long	preferredBlockSize;
	String 	clientName;
	String	clientMachine;
	String	clientNode;
	UserGroupInformation ugi;
	
	NNUStartFile(){ugi=UserGroupInformation.getCurrentUGI();}
	NNUStartFile(String src, PermissionStatus perm, short replication, 
			long preferredBlockSize, String clientName, String clientMachine, 
			DatanodeDescriptor clientNode, long timestamp){
		super(src,timestamp,perm);
		this.replication=replication;
		this.preferredBlockSize=preferredBlockSize;
		this.clientName=clientName;
		this.clientMachine=clientMachine;
		this.clientNode=(clientNode!=null)?clientNode.getStorageID():"";
		ugi=UserGroupInformation.getCurrentUGI();
	}
	public void readFields(DataInput in) throws IOException{
		super.readFields(in);
		ugi=new UnixUserGroupInformation();
		ugi.readFields(in);
		replication=in.readShort();
		preferredBlockSize=in.readLong();
		clientName=Text.readString(in);
		clientMachine=Text.readString(in);
		clientNode=Text.readString(in);
		if(clientNode.equals(""))
			clientNode=null;
	}
	public void write(DataOutput out) throws IOException {
		super.write(out);
		ugi.write(out);
		out.writeShort(replication);
		out.writeLong(preferredBlockSize);
		Text.writeString(out, clientName);
		Text.writeString(out, clientMachine);
		Text.writeString(out, clientNode);
	}

}

class NNUCloseNewFile extends NNUpdateUnit{
	static final short NNU_ABDF=16;//abandon file
	static final short NNU_CPLF=32;//complete file
	short op=0;
	String src;
	String holder;
	public void readFields(DataInput in) throws IOException{
		op=in.readShort();
		src=Text.readString(in);
		holder=Text.readString(in);
	}
	public void write(DataOutput out) throws IOException {
		out.writeShort(op);
		Text.writeString(out, src);
		Text.writeString(out, holder);
	}
}

class NNUMvRm extends NNUpdateUnit{
	String src;
	String dst;
	public void readFields(DataInput in) throws IOException{
		src=Text.readString(in);
		dst=Text.readString(in);
	}
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, src);
		Text.writeString(out, dst);
	}
}

class NNUINodeInfo extends NNUpdateUnit{
	static final short NNU_REP=1;
	static final short NNU_PERM=2;
	static final short NNU_OWNER=12; // 4 + 8
	short op=0;
	short replication=0;
	short perm=0;
	String user=null;
	String group=null;
	String src=null;
	
	NNUINodeInfo(){op=0;src="";}
	NNUINodeInfo(String src){op=0;this.src=src;}
	public void readFields(DataInput in) throws IOException{
		op=in.readShort();
		LOG.debug("inode modify type: [1,2,12] "+op);
		switch(op){
			case NNU_REP:
				LOG.debug("adjust replication");
				replication=in.readShort();
				break;
			case NNU_PERM:
				LOG.debug("modify the permission");
				perm=in.readShort();
				//perm=new FsPermission(p.getUserAction(),p.getGroupAction(),p.getOtherAction());
				break;
			case NNU_OWNER:
				LOG.debug("Change the owner");
				user=Text.readString(in);
				group=Text.readString(in);
				break;
		}
		src=Text.readString(in);
		LOG.debug("path is "+src);
	}
	public void write(DataOutput out) throws IOException {
		out.writeShort(op);
		LOG.debug("inode modify type: [1,2,12] "+op);
		switch(op){
		case NNU_REP:
			out.writeShort(replication);
			break;
		case NNU_PERM:
			out.writeShort(perm);
			break;
		case NNU_OWNER:
			Text.writeString(out, user);
			Text.writeString(out, group);
			break;
		}
		Text.writeString(out, src);
	}
}

/*
 * A DEntry inserted or removed; or block allocated or removed from dEntry;  
 */
class NNUpdateDEntry extends NNUpdateUnit{
	String basePath;	//dir
	boolean add;		//add or remove
	boolean file;		//file or subdir
	String inodeName;	//name of file or subdir
	LinkedList<String> blks;	//inserted blocks if any
	@Override
	public void readFields(DataInput in) throws IOException {
		basePath=Text.readString(in);
		add=in.readBoolean();
		file=in.readBoolean();
		inodeName=Text.readString(in);
		if(file){
			int blkCount=in.readInt();
			if(blkCount>0){
				blks=new LinkedList<String>();
				for(int i=0;i<blkCount;++i){
					blks.add(Text.readString(in));
				}
			}else{
				blks=null;
			}
		}
	}
	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, basePath);
		out.writeBoolean(add);
		out.writeBoolean(file);
		Text.writeString(out, inodeName);
		if(file){
			int blkCount=(blks!=null)?blks.size():0;
			if(blkCount>0){
				for(ListIterator<String> iter=blks.listIterator();iter.hasNext();){
					Text.writeString(out, iter.next());
				}
			}
		}
	}
}

class NNUDnodeReg extends NNUpdateUnit{
	DatanodeRegistration dnodeReg;
	public void readFields(DataInput in) throws IOException {
		dnodeReg=new DatanodeRegistration();
		dnodeReg.readFields(in);
	}
	public void write(DataOutput out) throws IOException {
		dnodeReg.write(out);
	}
}

class NNUDnodeHB extends NNUpdateUnit{
	DatanodeRegistration  dnode;
	long capacity;
	long dfsUsed;
	long remaining;
	int xceiverCount;
	int xmitsInProgress;
	
	public void readFields(DataInput in) throws IOException {
		dnode=new DatanodeRegistration();
		//dnode.readFields(in);
		dnode.readFields(in);
		//short padding;
		//padding=in.readShort();
		capacity=in.readLong();
		dfsUsed=in.readLong();
		remaining=in.readLong();
		xceiverCount=in.readInt();
		xmitsInProgress=in.readInt();
	}
	
	public void write(DataOutput out) throws IOException {
		//dnode.write(out);
		//DatanodeID dn=new DatanodeID(dnode.getName(),dnode.getStorageID(),dnode.getInfoPort(),dnode.getIpcPort());
		DatanodeRegistration d=new DatanodeRegistration();
		d.name=dnode.getName();
		d.storageID=dnode.getStorageID();
		d.ipcPort=dnode.getIpcPort();
		d.setInfoPort(dnode.getInfoPort());
		d.storageInfo=new StorageInfo(dnode.storageInfo);
		
		d.write(out);
		//short padding=0;
		//out.writeShort(padding);
		out.writeLong(capacity);
		out.writeLong(dfsUsed);
		out.writeLong(remaining);
		out.writeInt(xceiverCount);
		out.writeInt(xmitsInProgress);
	}
}

class NNUDnodeHBBatch extends NNUpdateUnit{
	Collection<NNUDnodeHB> hb;

	@Override
	public void readFields(DataInput in) throws IOException {
		hb=new LinkedList<NNUDnodeHB>();
		int hbcount=in.readInt();
		for(int i=0;i<hbcount;i++){
			NNUDnodeHB u=new NNUDnodeHB();
			u.readFields(in);
			hb.add(u);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if(hb==null||hb.size()==0){
			out.writeInt(0);
			return;
		}
		out.writeInt(hb.size());
		for(Iterator<NNUDnodeHB> iter=hb.iterator();iter.hasNext();){
			iter.next().write(out);
		}
	}
}

class NNUDnodeRm extends NNUpdateUnit{
	DatanodeID dnode;

	public void readFields(DataInput in) throws IOException {
		dnode=new DatanodeID();
		dnode.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		//dnode.write(out);
		DatanodeID d=new DatanodeID(dnode.getName(),dnode.getStorageID(),dnode.getInfoPort(),dnode.getIpcPort());
		d.write(out);

	}
	
}

class NNUpdateDnodeStatus extends NNUpdateUnit{
	String storageID;
	boolean add;
	LinkedList<String> replicateBlocks;
	LinkedList<String[]> replicateTargetSets;
	LinkedList<String> invalidateBlocks;
	@Override
	public void readFields(DataInput in) throws IOException {
		storageID=Text.readString(in);
		add=in.readBoolean();
		int repBlkCount=in.readInt();
		if(repBlkCount>0){
			replicateBlocks=new LinkedList<String>();
			replicateTargetSets=new LinkedList<String[]>();
			for(int i=0;i<repBlkCount;++i){
				replicateBlocks.add(Text.readString(in));
			}
			for(int i=0;i<repBlkCount;++i){
				int targetCount=in.readInt();
				if(targetCount>0){
					String targetSet[]=new String[targetCount];
					for(int j=0;j<targetCount;j++){
						targetSet[j]=Text.readString(in);
					}
					replicateTargetSets.add(targetSet);
				}
			}

		}else{
			replicateBlocks=null;
			replicateTargetSets=null;
		}
		int invBlkCount=in.readInt();
		if(invBlkCount>0){
			invalidateBlocks=new LinkedList<String>();
			for(int i=0;i<invBlkCount;++i){
				invalidateBlocks.add(Text.readString(in));
			}
		}else{
			invalidateBlocks=null;
		}
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		Text.writeString(out, storageID);
		out.writeBoolean(add);
		int repCount=(replicateBlocks!=null)?replicateBlocks.size():0;
		out.writeInt(repCount);
		if(repCount>0){
			for(ListIterator<String> iter=replicateBlocks.listIterator();iter.hasNext();){
				Text.writeString(out, iter.next());
			}
			for(ListIterator<String[]> iter=replicateTargetSets.listIterator();iter.hasNext();){
				String[] targetSet=iter.next();
				out.writeInt(targetSet.length);
				if(targetSet.length>0){
					for(int i=0;i<targetSet.length;++i){
						Text.writeString(out, targetSet[i]);
					}
				}
			}
		}
		int invCount=(replicateBlocks!=null)?invalidateBlocks.size():0;
		out.writeInt(invCount);
		if(invCount>0){
			for(ListIterator<String> iter=invalidateBlocks.listIterator();iter.hasNext();){
				Text.writeString(out, iter.next());
			}
		}
	}
	
	
}

class NNURplicaMon extends NNUpdateUnit{
	public void readFields(DataInput in) throws IOException {
	}

	public void write(DataOutput out) throws IOException {
	}
	
}

class NNUpdateLease extends NNUpdateUnit{
	String holder;
	boolean update=true;
	
	NNUpdateLease(){
		holder=null;
	}
	NNUpdateLease(String holder){
		this.holder=holder;
		this.update=true;
	}
	NNUpdateLease(String holder,boolean update){
		this.holder=holder;
		this.update=update;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		holder=Text.readString(in);
		update=in.readBoolean();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, holder);
		out.writeBoolean(update);
	}
	
}

class NNULeaseBatch extends NNUpdateUnit{
	Collection<NNUpdateLease> lb;

	@Override
	public void readFields(DataInput in) throws IOException {
		lb=new LinkedList<NNUpdateLease>();
		int hbcount=in.readInt();
		for(int i=0;i<hbcount;i++){
			NNUpdateLease u=new NNUpdateLease();
			u.readFields(in);
			lb.add(u);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if(lb==null||lb.size()==0){
			out.writeInt(0);
			return;
		}
		out.writeInt(lb.size());
		for(Iterator<NNUpdateLease> iter=lb.iterator();iter.hasNext();){
			iter.next().write(out);
		}
	}
}


class NNUpdateWorld extends NNUpdateUnit{
	FSDirectory dir;
	Map <String, DatanodeDescriptor> datanodeMap;
	BlocksMap blocksMap;
	
	public NNUpdateWorld(FSDirectory dir,Map <String, DatanodeDescriptor> datanodeMap,BlocksMap blocksMap){
	    this.dir = dir;
		this.datanodeMap = datanodeMap;
		this.blocksMap = blocksMap;
	}
	
	public void readFields(DataInput in) throws IOException{
	    //read dir
		int namespaceID=in.readInt();
		LOG.debug("namespaceID readed: "+namespaceID);
		if(namespaceID!=dir.fsImage.getNamespaceID()){
			throw new IOException("NNUpdateWorld: NamespaceID Mismatch!");
		}
		
		filesystem=new LinkedList<INodeDump>();
		int inodeCnt=in.readInt();
		LOG.debug("inode got "+inodeCnt);
		
		char roottype=in.readChar();
		if(roottype!='d'){
			LOG.error("root must be a dir!");
			throw new IOException("root is not dir...");
		}
		INodeDirectoryDump rootdir=new INodeDirectoryDump();
		rootdir.readFields(in);
		--inodeCnt;
		filesystem.add(rootdir);
		
		Map<String,INodeFileUnderConstructionDump> underconstructed=new TreeMap<String,INodeFileUnderConstructionDump>();
		
		int remain=readINodeIterative(in, inodeCnt, rootdir.childCnt, "", underconstructed);
		LOG.debug("remain unread INode: "+ remain);
		
		int datanodeCount=in.readInt();
		LOG.info("datanode got "+datanodeCount);
		datanodes=new LinkedList<DatanodeDump>();

		dir.namesystem.capacityTotal=0L;
		dir.namesystem.capacityUsed=0L;
		dir.namesystem.capacityRemaining=0L;
		dir.namesystem.totalLoad=0;
		dir.namesystem.clusterMap = new NetworkTopology();
		
		Configuration conf=new Configuration();
		
		dir.namesystem.replicator = new ReplicationTargetChooser(
				conf.getBoolean("dfs.replication.considerLoad", true),
                dir.namesystem,
                dir.namesystem.clusterMap);
		
		while(true){
                //for( ; 0 < datanodeCount; --datanodeCount) {
			DatanodeDump dnode=new DatanodeDump();
			dnode.readFields(in);
				
			LOG.debug("data got "+dnode.hostname);
			DatanodeDescriptor dd=new DatanodeDescriptor(dnode.id,
                    dnode.location,
                    dnode.hostname,
                    dnode.capacity,
                    dnode.dfsUsed,
                    dnode.remaining,
                    dnode.xceiverCount);
			//dd.xceiverCount=dnode.xceiverCount;
			dd.lastUpdate=dnode.lastUpdate;
			dd.isAlive=dnode.isAlive;
			//this.datanodeMap.put(dnode.id.getStorageID(), dd);
			
			dir.namesystem.resolveNetworkLocation(dd);
			dir.namesystem.unprotectedAddDatanode(dd);
			dir.namesystem.clusterMap.add(dd);
			
		    synchronized(dir.namesystem.heartbeats) {
		    	dir.namesystem.heartbeats.add(dd);
		        dd.isAlive = true;
		    }
			for(ListIterator<Block> iter=dnode.blocks.listIterator();iter.hasNext();){
				Block b=iter.next();
			    INodeFile fileINode = blocksMap.getINode(b);
			    int replication = (fileINode != null) ?  fileINode.getReplication() : 
			        dir.namesystem.getDefaultReplication();
				this.blocksMap.addNode(b, dd, replication);
			}
			
			dir.namesystem.capacityTotal+=dnode.capacity;
			dir.namesystem.capacityUsed+=dnode.dfsUsed;
			dir.namesystem.capacityRemaining+=dnode.remaining;
			dir.namesystem.totalLoad+=dnode.xceiverCount;
			
			//dir.namesystem.clusterMap.add(dd);
			
			if(--datanodeCount == 0){
		        		LOG.debug("transfer ended");
		        		break;
			}

		}
		
		/*
		 * Here we deal the under construction inodes last for they need the datanodeMap be established in advance.
		 */
		for(Iterator<String> iter=underconstructed.keySet().iterator();iter.hasNext();){
			String path=iter.next();
			INodeFileUnderConstructionDump inodeinfo=underconstructed.get(path);
			INodeFile inodefile=dir.getFileINode(path);
			DatanodeDescriptor[] targets=new DatanodeDescriptor[inodeinfo.targets.length];
			for(int i=0;i<inodeinfo.targets.length;i++){
				targets[i]=datanodeMap.get(inodeinfo.targets[i]);
			}
			INodeFileUnderConstruction inode=new INodeFileUnderConstruction(inodefile.name,
					inodefile.getReplication(),
					inodefile.getModificationTime(),
					inodefile.getPreferredBlockSize(),
					inodefile.getBlocks(),
					inodefile.getPermissionStatus(),
					inodeinfo.clientName,
					inodeinfo.clientMachine,
					this.datanodeMap.get(inodeinfo.clientNode));
			inode.setTargets(targets);
			dir.replaceNode(path, inodefile, inode);
			//rpy dir.namesystem.leaseManager.addLease(new StringBytesWritable(inodeinfo.clientName),path);
			dir.namesystem.leaseManager.addLease(inodeinfo.clientName,path);
		}
		underconstructed.clear();
		  		
		
	}
	
	int readINodeIterative(DataInput in, int inodeCnt, int childCount, String prefix, Map<String,INodeFileUnderConstructionDump> underconstructed) throws IOException{
		LOG.info("now in "+prefix+Path.SEPARATOR);
		for(int i=0;i<childCount;i++){
			INodeDump inode;
			char type=in.readChar();
			--inodeCnt;
			LOG.info("the next is "+type);
			if(type=='d'){
				inode=new INodeDirectoryDump();
				INodeDirectoryDump inodedir=(INodeDirectoryDump)inode;
				inode.readFields(in);
				filesystem.add(inode);
				LOG.info("trying insert " + prefix+Path.SEPARATOR+inode.name );
				dir.unprotectedAddFile(prefix+Path.SEPARATOR+inode.name,inode.perm,null,(short)0,inode.mtime,inode.atime,0L);
				inodeCnt=readINodeIterative(in,inodeCnt,inodedir.childCnt,prefix+Path.SEPARATOR+inode.name,underconstructed);
			}else if(type=='f'){
				inode=new INodeFileDump();
				INodeFileDump inodefile=(INodeFileDump)inode;
				inode.readFields(in);
				filesystem.add(inode);
				LOG.info("adding file "+prefix+Path.SEPARATOR+inodefile.name);
				LOG.info("file info: blockcount: " + inodefile.blocks.length + " blocksize " + inodefile.blocksize);
				dir.unprotectedAddFile(prefix+Path.SEPARATOR+inodefile.name, inodefile.perm,
			            inodefile.blocks, inodefile.replication, inodefile.mtime,inode.atime, inodefile.blocksize);
				LOG.info("added file "+prefix+Path.SEPARATOR+inodefile.name);
			}else if(type=='u'){//under construction
				inode=new INodeFileUnderConstructionDump();
				INodeFileUnderConstructionDump inodefile=(INodeFileUnderConstructionDump)inode;
				inode.readFields(in);
				filesystem.add(inode);
				dir.unprotectedAddFile(prefix+Path.SEPARATOR+inodefile.name, inodefile.perm,
			            inodefile.blocks, inodefile.replication, inodefile.mtime,inode.atime, inodefile.blocksize);
				underconstructed.put(prefix+Path.SEPARATOR+inodefile.name,inodefile);
			}else{
				throw new IOException("Wrong type of node readed when sync the world");
			}
		}
		return inodeCnt;
	}
	
	public void write(DataOutput out) throws IOException{
		//fsImage
		out.writeInt(dir.fsImage.namespaceID);
		LOG.debug("namespace ID sent "+dir.fsImage.namespaceID);
		
		out.writeInt(filesystem.size());
		LOG.info("inodes sent "+filesystem.size());
		
		for(ListIterator<INodeDump> iter=filesystem.listIterator();iter.hasNext();){
			INodeDump inode=iter.next();
			inode.write(out);
		}

	    //write datanodeMap
		out.writeInt(datanodes.size());
		LOG.info("datanodes sent "+datanodes.size());
		for(ListIterator<DatanodeDump> iter=datanodes.listIterator();iter.hasNext();){
			DatanodeDump dnode=iter.next();
			dnode.write(out);
			LOG.debug("datanode sent "+dnode.hostname);
		}
		
	}
	
	static class INodeDump implements Writable{
		String name;
		PermissionStatus perm;
		long mtime;
		long atime;
		
		INodeDump(){
			
		}
		
		INodeDump(INode orig){
			this.name=String.valueOf(orig.getLocalName());
			PermissionStatus p=orig.getPermissionStatus();
			this.perm=new PermissionStatus(p.getUserName(),p.getGroupName(),p.getPermission());
			this.mtime=orig.getModificationTime();
			this.atime=orig.getAccessTime();
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.name=Text.readString(in);
			PermissionStatus p=PermissionStatus.read(in);
			perm=new PermissionStatus(p.getUserName(),p.getGroupName(),p.getPermission());
			this.mtime=in.readLong();
			this.atime=in.readLong();
			LOG.info("Inode received: name "+name+" Perm: "+perm.toString()+" mtime: "+mtime);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			Text.writeString(out, this.name);
			perm.write(out);
			out.writeLong(mtime);
			out.writeLong(atime);
			LOG.info("Inode wrote: name "+name+" Perm: "+perm.toString()+" mtime: "+mtime);
		}
	}
	
	static class INodeDirectoryDump extends INodeDump{
		int childCnt;
		
		INodeDirectoryDump(){
			
		}
		INodeDirectoryDump(INodeDirectory dir){
			super(dir);
			childCnt=dir.getChildren().size();
		}
		
		public void readFields(DataInput in) throws IOException {
			super.readFields(in);
			this.childCnt=in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeChar('d');
			super.write(out);
			out.writeInt(childCnt);
		}
		
	}
	
	static class INodeFileDump extends INodeDump{
		short replication;
		long blocksize;
		//long[] blocks;
		Block[] blocks;
		
		INodeFileDump(){
			
		}
		INodeFileDump(INodeFile inode){
			super(inode);
			replication=inode.getReplication();
			blocksize=inode.getPreferredBlockSize();
			BlockInfo[] b=inode.getBlocks();
			blocks=new Block[b.length];
			for(int i=0;i<b.length;i++){
				blocks[i]=new Block(b[i].getBlockId(),b[i].getNumBytes(),b[i].getGenerationStamp());
			}
		}
		public void readFields(DataInput in) throws IOException {
			super.readFields(in);
			replication=in.readShort();
			blocksize=in.readLong();
			int bl=in.readInt();
			LOG.info("block count "+bl);
			blocks=new Block[bl];
			for(int i=0;i<bl;i++){
				blocks[i]=new Block();
				blocks[i].readFields(in);
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			if(this instanceof INodeFileUnderConstructionDump){
				out.writeChar('u');
			}else{
				out.writeChar('f');
			}
			super.write(out);
			out.writeShort(replication);
			out.writeLong(blocksize);
			out.writeInt(blocks.length);
			for(Block b : blocks){
				b.write(out);
			}
			DataOutputStream o=(DataOutputStream)out;
			o.flush();
		}

	}

	static class INodeFileUnderConstructionDump extends INodeFileDump{
		String clientName;
		String clientMachine;
		String clientNode;
		String[] targets;
		
		INodeFileUnderConstructionDump(){	
		}
		
		INodeFileUnderConstructionDump(INodeFileUnderConstruction inode) throws IOException{
			super(inode);
			clientName=inode.getClientName();
			clientMachine=inode.getClientMachine();
			DatanodeDescriptor  node=inode.getClientNode();
			clientNode= node==null?"":node.getStorageID();
			DatanodeDescriptor[] targetNodes=inode.getTargets();
			if(targetNodes!=null){
				targets=new String[targetNodes.length];
				for(int i=0;i<targetNodes.length;i++){
					targets[i]=targetNodes[i].getStorageID();
				}
			}else{
				targets=new String[0];
			}
		}
		public void readFields(DataInput in) throws IOException {
			super.readFields(in);
			clientName=Text.readString(in);
			clientMachine=Text.readString(in);
			clientNode=Text.readString(in);
			//LOG.info("client name "+ clientName + " machine: " + clientMachine + " node: " + clientNode);
			int targetLength=in.readInt();
			targets=new String[targetLength];
			for(int i=0;i<targetLength;i++){
				targets[i]=Text.readString(in);
			}
		}
		public void write(DataOutput out) throws IOException {
			super.write(out);
			Text.writeString(out, clientName);
			Text.writeString(out, clientMachine);
			Text.writeString(out, clientNode);
			out.writeInt(targets.length);
			for(int i=0;i<targets.length;i++){
				Text.writeString(out, targets[i]);
			}
		}

	}

	static class DatanodeDump implements Writable{
		
		DatanodeID id;
		
		long capacity;
		long dfsUsed;
		long remaining;
		long lastUpdate;
		int xceiverCount;
		String location;
		String hostname;
		
		boolean isAlive;
		
		List<Block> blocks;
		
		DatanodeDump(){
			id=new DatanodeID();
		}
		DatanodeDump(DatanodeDescriptor dnode){
			//DatanodeID
			this.id=new DatanodeID(dnode.getName(),dnode.getStorageID(),dnode.getInfoPort(),dnode.getIpcPort());
			
			//DatanodeInfo
			capacity=dnode.getCapacity();
			dfsUsed=dnode.getDfsUsed();
			remaining=dnode.getRemaining();
			lastUpdate=dnode.getLastUpdate();
			xceiverCount=dnode.getXceiverCount();
			location=dnode.getNetworkLocation();
			hostname=dnode.getHostName();
			//TODO: Decommission is not dealt with
			
			isAlive=dnode.isAlive;
			
			//DatanodeDescriptor
			blocks=new LinkedList<Block>();
			for(Iterator<Block> iter=dnode.getBlockIterator();iter.hasNext();){
				Block b=iter.next();
				blocks.add(new Block(b.getBlockId(),b.getNumBytes(),b.getGenerationStamp()));
			}
			//TODO: working list is not processed
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			id.readFields(in);
			LOG.debug("datanode ID "+id.getStorageID());
			capacity=in.readLong();
			LOG.debug("datanode capacity "+capacity);
			dfsUsed=in.readLong();
			LOG.debug("datanode dfsUsed "+dfsUsed);
			remaining=in.readLong();
			LOG.debug("datanode remaining "+remaining);
			lastUpdate=in.readLong();
			LOG.debug("datanode lastUpdate "+lastUpdate);
			xceiverCount=in.readInt();
			LOG.debug("datanode xceiverCount "+xceiverCount);
			location=Text.readString(in);
			LOG.debug("datanode location "+location);
			hostname=Text.readString(in);
			LOG.debug("datanode hostname "+hostname);
			
			isAlive=in.readBoolean();
			
			int blockCount=in.readInt();
			LOG.debug("datanode block count "+blockCount);
			blocks=new LinkedList<Block>();
			for(int i=0;i<blockCount;i++){
				Block b=new Block();
				b.readFields(in);
				blocks.add(b);
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			id.write(out);
			out.writeLong(capacity);
			out.writeLong(dfsUsed);
			out.writeLong(remaining);
			out.writeLong(lastUpdate);
			out.writeInt(xceiverCount);
			Text.writeString(out, location);
			Text.writeString(out, hostname);
			LOG.debug("datanode ID "+id.getStorageID());
			LOG.debug("datanode capacity "+capacity);
			LOG.debug("datanode dfsUsed "+dfsUsed);
			LOG.debug("datanode remaining "+remaining);
			LOG.debug("datanode lastUpdate "+lastUpdate);
			LOG.debug("datanode xceiverCount "+xceiverCount);
			LOG.debug("datanode location "+location);
			LOG.debug("datanode hostname "+hostname);
			
			out.writeBoolean(isAlive);
			
			out.writeInt(blocks.size());
			LOG.debug("datanode block count "+blocks.size());
			for(ListIterator<Block> iter=blocks.listIterator();iter.hasNext();){
				iter.next().write(out);
			}
		}
		
	}
	
	List<INodeDump> filesystem;
	List<DatanodeDump> datanodes;

	void dumpWorld(){
		dumpRootDir();
		dumpDatanode();
	}
	
	void dumpRootDir(){
		INodeDirectory root=dir.rootDir;
		filesystem=new LinkedList<INodeDump>();
		dumpDirIterative(root);
	}
	
	void dumpDirIterative(INodeDirectory dir){
		filesystem.add(new INodeDirectoryDump(dir));
		LOG.info("dump a dir " + dir.getLocalName() + " with " + dir.getChildren().size() + " children.");
		for(INode child: dir.getChildren()){
			if(child.isDirectory()){
				dumpDirIterative((INodeDirectory)child);
			}else if(!child.isUnderConstruction()){
				filesystem.add(new INodeFileDump((INodeFile)child));
			}else{
				try{
				filesystem.add(new INodeFileUnderConstructionDump((INodeFileUnderConstruction)child));
				}catch(IOException ie){
					LOG.debug("exception while dump an inodefileunderconstruction");
				}
			}
		}
	}
	
	void dumpDatanode(){
		datanodes=new LinkedList<DatanodeDump>();
		for(Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); it.hasNext();) {
	        DatanodeDescriptor node = it.next();
			datanodes.add(new DatanodeDump(node));
		 }
		
	}
	
}
