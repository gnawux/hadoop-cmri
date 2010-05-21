package org.apache.hadoop.hdfs.server.namenode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class NNUpdateInfo implements Writable {
	static final int NNU_NOP=0;  	// nothing to do 
	static final int NNU_BLK=1;		// add or remove a block
	static final int NNU_INODE=3;   // add or remove or modify an inode (add or remove file; new block allocation)
	static final int NNU_NEWFILE=4;	// start new file
	static final int NNU_CLSFILE=5;	// close new file ..x (not used)
	static final int NNU_MVRM=6;	// close new file ..x
	static final int NNU_MKDIR=7;	// close new file 
	static final int NNU_LEASE=16;	// add/update or release a lease
	static final int NNU_REPLICAMON=17; //replication monitor work
	static final int NNU_LEASE_BATCH=18;	// update batch of leases
	static final int NNU_DNODE=32;	// add or remove a datanode (register or no heartbeat and deleted) ..x (not used)
	static final int NNU_DNODEREG=33;	// dnode register 
	static final int NNU_DNODEHB=34;	// heart beat received
	static final int NNU_DNODEBLK=35;	// block report
	static final int NNU_BLKRECV=36;
	static final int NNU_DNODERM=37;   // remove dnode
	static final int NNU_DNODEHB_BATCH=38;  //batch of heartbeat
	static final int NNU_WORLD=127;  // refresh the whole world ...x (not used)
	static final int NNU_MASSIVE=255;
	
	static final short NNF_UPDATE = 0;
	static final short NNF_MASS = 1;
	static final short NNF_WORLD = 2;
	static final short NNF_RESERVED = -1;

	static long seq=0L;
		
	long 	timeStamp;
	String 	updateID;
	int 	updateType;
	short 	updateFlag = NNF_UPDATE;
	long 	ic = 0; //reserved for a checksum used in integration check in future. not used now 
	
	public NNUpdateInfo(){
		timeStamp=System.currentTimeMillis();
		updateID="U_"+timeStamp+"_"+seq;
		++seq;
		updateType=NNU_NOP;
	}

	public NNUpdateInfo(int updateType){
		timeStamp=System.currentTimeMillis();
		updateID="U_"+timeStamp+"_"+seq;
		this.updateType=updateType;
		++seq;
	}
	
	public NNUpdateInfo(NNUpdateInfo o){
		timeStamp=o.timeStamp;
		updateID=new String(o.updateID);
		updateType=o.updateType;
		updateFlag=o.updateFlag;
		ic=o.ic;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		timeStamp=in.readLong();
		updateID=Text.readString(in);
		updateType=in.readInt();
		updateFlag=in.readShort();
		ic = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(timeStamp);
		Text.writeString(out, updateID);
		out.writeInt(updateType);
		out.writeShort(updateFlag);
		out.writeLong(ic);
	}
}
