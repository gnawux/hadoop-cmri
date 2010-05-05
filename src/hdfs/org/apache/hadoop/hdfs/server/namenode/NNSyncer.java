/**
 * As a member of FSNamesystem, to sync the members of it
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.zookeeper.NNKeeper;
import org.apache.hadoop.net.NetUtils;

/**
 * @author gnawux
 *
 */
public class NNSyncer{
	
	public static final Log LOG = LogFactory.getLog("org.apache.hadoop.dfs.NNSyncer");
	protected static final Random R = new Random();

	/*
	 * Update status:
	 */
	static final int NNUS_EMPTY=-1;
	static final int NNUS_UPTODATE=0;
	static final int NNUS_SYNCED=1;
	static final int NNUS_UNSYNCED=2;
	
	//parameters:
	int socketWriteTimeout;
	int socketTimeout;
	
	public boolean disabled=false;
	boolean shouldRecordUpdate=false;
	
	SortedMap<String, NNUpdateEncap> updateQueue;

	NameNode namenode; //need it
	FSNamesystem namesystem;
	InetSocketAddress syncMasterAddress;
	
	protected NNKeeper nnkeeper;
	
	public NNSyncer(NameNode nn, FSNamesystem namesystem, Configuration conf) throws IOException{
		initialize(nn,namesystem,conf);
	}
	
	public void changeSyncMaster(String newMaster) throws IOException{
		LOG.info("New NameNode activated " + newMaster);
	}
	
	private void initialize(NameNode nn, FSNamesystem namesystem, Configuration conf) throws IOException{
		namenode=nn;
		this.namesystem=namesystem;
		
		nnkeeper=new NNKeeper(conf);
		nnkeeper.setMasterTracker(this);
	}
	
	public static URI getAvailableURI(Configuration conf) throws IOException{
		String machineName=InetAddress.getLocalHost().getCanonicalHostName();
		URI master=FileSystem.getDefaultUri(conf);
		LOG.info("machine name: "+machineName);
		
		try {
			return new URI(master.getScheme(),master.getUserInfo(),machineName,master.getPort(),master.getPath(),master.getQuery(),master.getFragment());
		} catch (URISyntaxException e) {
			LOG.fatal("unable to verify the URI of the namenode", e);
			System.exit(-1);
		}
		return null;
	}
	
	public static InetSocketAddress getMasterNameNodeAddress(Configuration conf){
		boolean nncEnabled = isNNCEnabled(conf);
		
		if(!nncEnabled)
			return NetUtils.createSocketAddr(FileSystem.getDefaultUri(conf).getAuthority(), NameNode.DEFAULT_PORT);
		
		try {
			NNKeeper nnkeeper=new NNKeeper(conf);
			return  AddrFromString( nnkeeper.syncGetMasterNameNode(10000L) );
		} catch (IOException e) {
			LOG.error("Exception when try to fetch the namenode address ", e);
			return null;
		}
	}
	
	public static boolean isNNCEnabled(Configuration conf){
		return conf.getBoolean("dfs.nnc.enabled", true);
	}
	
	public static List<URI> getSlaveURI(Configuration conf){
		List<URI> slaveURI=new ArrayList<URI>();
		String[] slaves=conf.get("fs.slaves.name","").split(",");
		for(int i=0;i<slaves.length;++i){
			slaveURI.add(URI.create(slaves[i]));
		}
		return slaveURI;
	}

	public static String hostFromString(String address){
		String res=null;
		
		if( address != null ){
			String[] segments=address.split(":");
			if( segments.length == 2 ){
				res=segments[0];
			}
		}
			
		return res;
	}

	public static int portFromString(String address){
		int res=-1;
		
		if( address != null ){
			String[] segments=address.split(":");
			if( segments.length == 2 ){
				res=Integer.valueOf(segments[1]);
			}
		}
			
		return res;
	}
	
	public static InetSocketAddress AddrFromString(String address){
		InetSocketAddress res=null;
		
		if( address != null ){
			String[] segments=address.split(":");
			if( segments.length == 2 ){
				res=new InetSocketAddress(segments[0],Integer.valueOf(segments[1]));
			}
		}
			
		return res;
	}

}

