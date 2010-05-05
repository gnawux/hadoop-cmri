package org.apache.hadoop.hdfs.zookeeper;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface DualWayNameNode extends  AlwaysOnNameNode{
	public InetSocketAddress altAddress() throws IOException;
	public VersionedProtocol altNameNode() throws IOException;
}