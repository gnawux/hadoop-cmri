package org.apache.hadoop.hdfs.server.namenode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

public class NNUpdateEncap implements Writable{
	
	public static final Log LOG = LogFactory.getLog("org.apache.hadoop.dfs.NNUpdateEncap");
	
	NNUpdateInfo head;
	NNUpdateUnit body;
	
	public NNUpdateEncap(){
		head=new NNUpdateInfo();
		body=null;
	}
	
	public NNUpdateEncap(NNUpdateInfo head, NNUpdateUnit body){
		this.head=head;
		this.body=body;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		LOG.debug("Ready to read head");
		head.readFields(in);
		LOG.info("head readed: "+head.updateID+" Type: "+head.updateType);
		body=NNUpdateUnit.read(in, head.updateType);	
		LOG.debug("body readed");
	}

	@Override
	public void write(DataOutput out) throws IOException {
		head.write(out);
		if(body != null)
			body.write(out);
	}


}


