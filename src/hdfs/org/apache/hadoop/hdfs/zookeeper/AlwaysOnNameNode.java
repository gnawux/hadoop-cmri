package org.apache.hadoop.hdfs.zookeeper;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface AlwaysOnNameNode {
	public void init()  throws IOException;
	public InetSocketAddress address() throws IOException;
	public VersionedProtocol namenode() throws IOException; 
	public void stop();
}