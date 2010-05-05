/**
 * 
 */
package org.apache.hadoop.hdfs.zookeeper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.NNSyncer;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
//import org.apache.zookeeper.data.*;

/**
 * @author wangxu
 *
 */
public class NNKeeper{
	
	public static final Log LOG = LogFactory.getLog("org.apache.hadoop.hdfs.nnkeeper");
	
	ZooKeeper	zk;
	String		znode;
	
	String		master;
	List<String>	list=new LinkedList<String>();
	
	boolean		masterAvailable=false;
	boolean		listAvailable=false;
	
	NNSyncer	syncAgent=null;
	
	public NNKeeper(Configuration conf) throws IOException{
		if(conf==null){
			conf=new Configuration();
		}
		
		String host=conf.get("dfs.nnc.zkserver","127.0.0.1:2181");
		String zn=conf.get("dfs.nnc.zknode","/nnc/default");
		
		initialize(host, zn);
	}
	
	public NNKeeper(String host, String zn) throws IOException{
		initialize(host,zn);
	}
	
	public void close(){
		try {
			zk.close();
		} catch (InterruptedException e) {
			// ignore
		}
	}
	
	public void setMasterTracker(NNSyncer tracker){
		syncAgent=tracker;
	}
	
	private void initialize(String host, String znode) throws IOException{
		this.znode=znode;
		this.zk=new ZooKeeper(host, 150000, zkWatcher);
		
		try {
			createNode(this.znode+"/namenodes");
			createNode(this.znode+"/master");
		} catch (KeeperException e) {
			LOG.warn("KeeperException when try to find the existance and create znode", e);
			throw new IOException(e);
		} catch (InterruptedException e) {
			LOG.warn("InterruptedException when try to find the existance and create znode", e);
			throw new IOException(e);
		}
	}
	
	MasterWatcher masterWatcher=new MasterWatcher();
	ListWatcher listWatcher=new ListWatcher();

	public void fetchMasterNameNode(){
		if(masterWatcher==null)
			masterWatcher=new MasterWatcher();
		
		LOG.debug("nnc, called NNKeeper.fetchMasterNameNode(), will set masterAvailable to false and call k.exists(znode+/master, masterWatcher, masterWatcher, null)");
		masterAvailable=false;
		
		zk.exists(znode+"/master", masterWatcher, masterWatcher, null);
	}
	
	public String getMasterNameNode(){
		if(masterWatcher==null)
			masterWatcher=new MasterWatcher();
		
		synchronized(masterWatcher){
			return masterAvailable ? master:null;
		}
	}
	
	public String syncGetMasterNameNode(long timeout){
		if(masterAvailable==true)
			return master;
		
		fetchMasterNameNode();
		synchronized(masterWatcher){
			while(!masterAvailable){
				try {
					masterWatcher.wait(timeout);
				} catch (InterruptedException e) {
					// ignore
				}
			}
		}
		return master;
	}
	
	public void fetchNameNodeList(){
		if(listWatcher==null)
			listWatcher=new ListWatcher();
		
		listAvailable=false;
		
		zk.getChildren(znode+"/namenodes", listWatcher, listWatcher, null);
	}
	
	public List<String> getNameNodeList(){
		if(listWatcher==null)
			listWatcher=new ListWatcher();
		
		synchronized(listWatcher){
			return listAvailable ? list : new LinkedList<String>();
		}
	}
	
	public List<String> syncGetNameNodeList(long timeout){
		if(listAvailable)
			return list;
		
		fetchNameNodeList();
		synchronized(listWatcher){
			while(!listAvailable){
				try {
					listWatcher.wait(timeout);
				} catch (InterruptedException e) {
					// ignore
				}
			}
		}
		return list;
	}
	
	public void setMasterNameNode(String master){
		if(masterWatcher==null)
			masterWatcher=new MasterWatcher();

		LOG.debug("nnc, called NNKeeper.setMasterNameNode(String master), master is " + master + ", will set  masterAvailable to false and call zk.exists(znode+/master, masterWatcher, masterWatcher, master)");

		masterAvailable=false;

		zk.exists(znode+"/master", masterWatcher, masterWatcher, master);
	}
	
	public void addNameNode(String nn){
		if(listWatcher==null)
			listWatcher=new ListWatcher();

        LOG.debug("nnc, called NNKeeper.addNameNode(String nn), nn is " + nn + ", will set listAvailable to false and call zk.getChildren(znode+/namenodes, listWatcher, listWatcher, +nn )");
		
		listAvailable=false;

		zk.getChildren(znode+"/namenodes", listWatcher, listWatcher, "+" + nn );
	}
	
	public void removeNameNode(String nn){
		if(listWatcher==null)
			listWatcher=new ListWatcher();
		
        LOG.debug("nnc, called NNKeeper.removeNameNode(String nn), nn is " + nn + ", will set listAvailable to false and call zk.getChildren(znode+/namenodes, listWatcher, listWatcher, -nn )");

		listAvailable=false;

		zk.getChildren(znode+"/namenodes", listWatcher, listWatcher, "-"+nn);
	}
	
	private void createNode(String path) throws IOException, KeeperException, InterruptedException {
		if(path==null || path.length()<=1 || path.indexOf('/')!=0){
			throw new IOException("invalid znode name: "+ path);
		}
		
		String[] nodes=path.split("/");
		String zn="";
		for(String n:nodes){
			if( n==null || n.equals("") )
				continue;
			
			zn = zn+"/"+n;
			if(zk.exists(zn, false)==null)
				zk.create(zn, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}
	
	ZKWatcher zkWatcher=new ZKWatcher();
	class ZKWatcher implements Watcher {
		public void process(WatchedEvent event){
			LOG.debug("nnc, called ZKWatcher.process(WatchedEvent event), event type is " + event.getType() + "event state is " + event.getState());
		}
	}
	
	class MasterWatcher implements AsyncCallback.StatCallback, AsyncCallback.DataCallback, Watcher {



		public void process(WatchedEvent event) {
			Event.EventType type=event.getType();
			Event.KeeperState stat=event.getState();
			
			String prompt = "unknown event or state";
			//TODO: SyncConnected should be handled, it may be required to check whether the master is different from befor. and may triger a change operation.
			if(type == Event.EventType.NodeCreated || type == Event.EventType.NodeDataChanged )
			//		|| (type == Event.EventType.None && stat == Event.KeeperState.SyncConnected))
			{
				if(type == Event.EventType.NodeCreated){
					prompt = "type is Event.EventType.NodeCreated";
				}
				else{
					prompt = "type is Event.EventType.NodeDataChanged";
				}
				LOG.debug("nnc, called MasterWatcher.process(WatchedEvent event), event type is " + prompt + ", will call zk.getData(znode+/master, this, this, syncAgent)");
				zk.getData(znode+"/master", this, this, syncAgent);
			}else if(type == Event.EventType.NodeDeleted ){
				prompt = "type is Event.EventType.NodeDeleted";
				LOG.debug("nnc, called MasterWatcher.process(WatchedEvent event), event type is " + prompt + ", will set masterAvailable to false and call zk.exists(znode+/master, this, this, null);");
				masterAvailable=false;
				zk.exists(znode+"/master", this, this, null);
			}else if(type == Event.EventType.None && stat == Event.KeeperState.Expired){
				prompt = "type is Event.EventType.None and state is Event.KeeperState.Expired";
				LOG.debug("nnc, called MasterWatcher.process(WatchedEvent event), event type is " + prompt +", will set masterAvailable to false");
				masterAvailable=false;
			}
		}

		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			if(rc==0){
				if(ctx==null){ // ctx is a string if exists is called by setMaster
					LOG.debug("nnc, called MasterWatcher.AsyncCallback.StatCallback.processResult(...), will call zk.getData(path, this, this, null), path is " + path);
					zk.getData(path, this, this, null);
				}else{
					LOG.debug("nnc, called MasterWatcher.AsyncCallback.StatCallback.processResult(...), will call zk.setData(znode+/master, masterAddress.getBytes(), -1, this, null), masterAddress.getBytes is " + (String)ctx);
					String masterAddress=(String)ctx;
					zk.setData(znode+"/master", masterAddress.getBytes(), -1, this, null);
				}
			}else{
				LOG.debug("nnc, called MasterWatcher.AsyncCallback.StatCallback.processResult(...), will masterAvailable to false");
				masterAvailable=false;
			}
		}

		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data,
				Stat stat) {
			if( rc == 0 && data != null && data.length > 0 ){
					synchronized(this){
						master=new String(data);
						masterAvailable=true;
						this.notifyAll();
					}
					LOG.info("nnc, called MasterWatcher.AsyncCallback.DataCallback.processResult(...), new master is " + master + " and masterAvailable is true");
					if( ctx != null){//
                                            LOG.warn("NONONO! WhyWhat?"); 
			                //rpy 20100120		NNSyncer tracker=(NNSyncer)ctx;
				        //		try{
				        //			tracker.changeSyncMaster(master);
				        //		} catch(IOException e) {
				        //			LOG.warn("exception while changeSyncMaster to " + master,e);
				        //		}
					}
			}else{
				masterAvailable=false;
			}
		}
		
	
	}
	
	class ListWatcher implements AsyncCallback.ChildrenCallback,AsyncCallback.StringCallback, AsyncCallback.VoidCallback, Watcher {



		public void process(WatchedEvent event) {
			LOG.info("list event " + event.getType());

			Event.EventType type=event.getType();
			Event.KeeperState stat=event.getState();
			
			String prompt = "unknown event";
			if(type == Event.EventType.NodeChildrenChanged
					|| (type == Event.EventType.None && stat == Event.KeeperState.SyncConnected)){
				if( type == Event.EventType.NodeChildrenChanged ){
					prompt = "event type is Event.EventType.NodeChildrenChanged";
				}
				else{
					prompt = "event type is Event.EventType.None and event state is Event.KeeperState.SyncConnected";
				}
				LOG.debug("nnc, called ListWatcher.process(WatchedEvent), " + prompt + ", will call zk.getChildren(znode+/namenodes, this, this, null)");
				zk.getChildren(znode+"/namenodes", this, this, null);
			}else if(type == Event.EventType.None && stat == Event.KeeperState.Expired ){
				prompt = "event type is Event.EventType.None and state is Event.KeeperState.Expired";
				LOG.debug("nnc, called ListWatcher.process(WatchedEvent), " + prompt + ", will set listAvailable to false");
				listAvailable=false;
			}
		}

		@Override
		public void processResult(int rc, String path, Object ctx,
				List<String> children) {
			LOG.info("list children with result " + rc);
			LOG.debug("nnc, called ListWatcher.AsyncCallback.ChildrenCallback.processResult(...) ++++++");
			if(rc==0){
				if( children!=null ){
					synchronized(this){
						list=children;
						if( ctx == null && children.size()>0 ){
							LOG.debug("------ called ListWatcher.AsyncCallback.ChildrenCallback.processResult(...), will set listAvailable to true");
							listAvailable=true;
							this.notifyAll();
						}
					}
					if(ctx!=null){
						String op=(String)ctx;
						int opflag=op.charAt(0);
						String address=op.substring(1);
						boolean exist=children.contains(address);
						if(!exist && opflag == '+'){
							LOG.debug("------ called ListWatcher.AsyncCallback.ChildrenCallback.processResult(...), +++ +++");
							LOG.debug("------------ will call zk.create(znode+/namenodes/+address, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, this, ctx), address is " + address + "ctx is " + op);
							zk.create(znode+"/namenodes/"+address, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, this, ctx);
						}else if(exist && opflag == '-'){
							LOG.debug("------ called ListWatcher.AsyncCallback.ChildrenCallback.processResult(...), +++ +++");
							LOG.debug("------------ will call zk.delete(znode+/namenodes/+address, -1, this, ctx); address is " + address + "ctx is " + op);
							zk.delete(znode+"/namenodes/"+address, -1, this, ctx);
						}
						LOG.info( "start " + (String)ctx );
					}
					return;
				}
			}
			synchronized(this){
				list.clear();
			}
		}

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			//LOG.info( "done " + (String)ctx );
			LOG.debug("nnc, called ListWatcher.AsyncCallback.StringCallback.processResult(rc, path, ctx, name), rc = " + rc + "path is " + path + "ctx is " + (String)ctx + "name is " + name);
			if(rc!=0){
				LOG.warn("NameNode operation " + (String)ctx + " failed with code " + rc);
			}
		}

		@Override
		public void processResult(int rc, String path, Object ctx) {
			//LOG.info( "done " + (String)ctx );
			LOG.debug("nnc, called ListWatcher.AsyncCallback.VoidCallback.processResult(rc, path, ctx), rc = " + rc + "path is " + path + "ctx is " + (String)ctx);
			if(rc!=0){
				LOG.warn("NameNode operation " + (String)ctx + " failed with code " + rc);
			}
		}
		
	
	}
	
	private static void printUsage(){
		System.out.println("Syntax: NNKeeper <host> <znode> <op> [other needed arguements...]");
		System.out.println("\t<host>: server1:port1,server2:port2,...,serverN:portN");
		System.out.println("\t<znode>: /nnc/identifi_string");
	}
	
	public static void main(int argc, String[] argv){
		if(argc <= 2){
			printUsage();
		}
		
		try {
			NNKeeper nnkeeper=new NNKeeper(argv[0],argv[1]);
			if(argv[2]=="master"){
				if(argc>3)
					nnkeeper.setMasterNameNode(argv[3]);
				
				String m=nnkeeper.syncGetMasterNameNode(10000L);
				System.out.println("master: "+m);
			}else if(argv[2]=="show"){
				
				List<String> nl=nnkeeper.syncGetNameNodeList(10000L);
				StringBuffer strbuf=new StringBuffer();
				for(String nn:nl){
					strbuf.append(' ');
					strbuf.append(nn);
				}
				System.out.println(strbuf.toString());
				
			}else if(argv[2]=="insert" && argc > 3){
				nnkeeper.removeNameNode(argv[3]);
				
				List<String> nl=nnkeeper.syncGetNameNodeList(10000L);
				StringBuffer strbuf=new StringBuffer();
				for(String nn:nl){
					strbuf.append(' ');
					strbuf.append(nn);
				}
				System.out.println(strbuf.toString());
			}else if(argv[2]=="delete" && argc > 3){
				nnkeeper.removeNameNode(argv[3]);
				
				List<String> nl=nnkeeper.syncGetNameNodeList(10000L);
				StringBuffer strbuf=new StringBuffer();
				for(String nn:nl){
					strbuf.append(' ');
					strbuf.append(nn);
				}
				System.out.println(strbuf.toString());
			}else{
				printUsage();
			}
			
			nnkeeper.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}
}