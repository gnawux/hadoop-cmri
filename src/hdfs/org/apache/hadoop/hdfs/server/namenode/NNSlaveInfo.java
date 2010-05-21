package org.apache.hadoop.hdfs.server.namenode;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;

public class NNSlaveInfo extends NNSlaveID {
	int 	updateStatus;
	String 	updateID;
	
	DataInputStream in=null;
	DataOutputStream out=null;
	
	public NNSlaveInfo(){
		this.updateID=new String("");
		this.updateStatus=NNSyncer.NNUS_EMPTY;
	}
	public NNSlaveInfo(String name, String updateID){
		super(name,"",-1);
		this.updateID=updateID;
		this.updateStatus=NNSyncer.NNUS_EMPTY;
	}
	public String getUpdateID(){
		return this.updateID;
	}
	public int getUpdateStatus(){
		return this.updateStatus;
	}
	public void setUpdateID(String updateID){
		this.updateID=updateID;
	}
	public void setUpdateStatus(int status){
		this.updateStatus=status;
	}
	/*
	 * Writable
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		updateStatus=in.readInt();
		updateID=Text.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(updateStatus);
		assert updateID != null : "null string!";
		Text.writeString(out, updateID);
	}

}
