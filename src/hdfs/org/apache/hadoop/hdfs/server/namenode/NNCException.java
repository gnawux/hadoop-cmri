package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

class NNCException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	NNCException(String message){
		super(message);
	}

}
