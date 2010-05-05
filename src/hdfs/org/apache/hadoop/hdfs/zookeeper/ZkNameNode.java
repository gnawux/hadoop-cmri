package org.apache.hadoop.hdfs.zookeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.zookeeper.NNKeeper.ZKWatcher;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZkNameNode implements AlwaysOnNameNode{

	protected Class<?> 	protocol;
	protected long	protocolVersion;
	
	protected UnixUserGroupInformation 		ugi=null;
	protected Map<String,RetryPolicy> 		methodNameToPolicyMap=null;
	
	protected InetSocketAddress nnAddr=null;
	protected VersionedProtocol nnrpc=null;
	protected VersionedProtocol nn=null;
	protected String			nnName=null;
	
	protected MasterWatcher masterWatcher=new MasterWatcher();
	
	protected Configuration conf;
	protected long			timeout=30000L;
	
	protected boolean		masterAvailable=false;

	public static final Log LOG = LogFactory.getLog("org.apache.hadoop.hdfs.zookeeper");
	
	ZooKeeper	zk;
	String		znode;
	
	
	public ZkNameNode(Class<?> proto, long version, Configuration cf) throws IOException{
		protocol=proto;
		protocolVersion=version;
		conf=cf;
		
		if(conf==null){
			conf=new Configuration();
		}
		
		String host=conf.get("dfs.nnc.zkserver","127.0.0.1:2181");
		String zn=conf.get("dfs.nnc.zknode","/yahoonnc/default");
		
		initialize(host, zn);

	}

	public ZkNameNode(Class<?> proto, long version, Configuration cf, UnixUserGroupInformation ugi, Map<String,RetryPolicy> methodNameToPolicyMap) throws IOException{
		protocol=proto;
		protocolVersion=version;
		conf=cf;
		this.ugi=ugi;
		this.methodNameToPolicyMap=methodNameToPolicyMap;
		
		if(conf==null){
			conf=new Configuration();
		}
		
		String host=conf.get("dfs.nnc.zkserver","127.0.0.1:2181");
		String zn=conf.get("dfs.nnc.zknode","/yahoonnc/default");
		
		initialize(host, zn);

	}

	public void init() throws IOException{
		fetchMasterNameNode();
	}
	
	public VersionedProtocol namenode() throws IOException {
		if(masterAvailable==true)
			return nn;
		
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
		return nn;	
	}
	
	public InetSocketAddress address() throws IOException {
		if(masterAvailable==true)
			return nnAddr;
		
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
		return nnAddr;	
	}
	
	public void stop(){
		if( nnrpc != null )
			RPC.stopProxy(nnrpc);
		
		try {
			zk.close();
		} catch (InterruptedException e) {
			// ignore
		}
	}
	
	protected void initialize(String host, String znode) throws IOException{
		this.znode=znode;
		this.zk=new ZooKeeper(host, 150000, zkWatcher);
		
//		try {
//			createNode(this.znode+"/namenodes");
//		} catch (KeeperException e) {
//			LOG.warn("KeeperException when try to find the existance and create znode", e);
//			throw new IOException(e);
//		} catch (InterruptedException e) {
//			LOG.warn("InterruptedException when try to find the existance and create znode", e);
//			throw new IOException(e);
//		}
	}

//	protected void createNode(String path) throws IOException, KeeperException, InterruptedException {
//		if(path==null || path.length()<=1 || path.indexOf('/')!=0){
//			throw new IOException("invalid znode name: "+ path);
//		}
//		
//		String[] nodes=path.split("/");
//		String zn="";
//		for(String n:nodes){
//			if( n==null || n.equals("") )
//				continue;
//			
//			zn = zn+"/"+n;
//			if(zk.exists(zn, false)==null)
//				zk.create(zn, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//		}
//	}
	
	protected void fetchMasterNameNode(){
		masterAvailable=false;
		zk.exists(znode+"/master", masterWatcher, masterWatcher, null);
	}

	protected VersionedProtocol newRPCProxy(InetSocketAddress addr) throws IOException{
		VersionedProtocol rpcProxy=null;
		if(ugi==null)
			rpcProxy = RPC.waitForProxy(protocol, protocolVersion, addr, conf);
		else
			rpcProxy = RPC.getProxy(protocol, protocolVersion, addr, ugi, conf,
			        NetUtils.getSocketFactory(conf, protocol));
		
		return rpcProxy;
	}
	
	protected VersionedProtocol newProxy(VersionedProtocol rpcProxy){
		if( methodNameToPolicyMap == null)
			return rpcProxy;
		else
			return (VersionedProtocol) RetryProxy.create( protocol, rpcProxy, methodNameToPolicyMap);
	}
	
	ZKWatcher zkWatcher=new ZKWatcher();
	class ZKWatcher implements Watcher {
		public void process(WatchedEvent event){
			//trivial
            Event.EventType type = event.getType();
            Event.KeeperState stat = event.getState();

			String eventString = "unknown event";
            if( type == Event.EventType.NodeCreated ){
            	eventString = "Event.EventType.NodeCreated";
            }
            else if( type == Event.EventType.NodeDataChanged ) {
            	eventString = "Event.EventType.NodeDataChanged";
            }
            else if( type == Event.EventType.None ) {
                eventString = "Event.EventType.None";
            } 
            else if(type == Event.EventType.NodeDeleted ) {
                eventString = "Event.EventType.NodeDeleted";
            }
            else if( stat == Event.KeeperState.SyncConnected ) {
                eventString = "Event.KeeperState.SyncConnected";
            }
            else if( stat == Event.KeeperState.Expired) {
                eventString = "Event.KeeperState.Expired";
            }
            LOG.debug("nnc, in zkWatcher process method, (WatchedEvent)event:" + eventString);
		}
	}

	protected class MasterWatcher implements AsyncCallback.StatCallback, AsyncCallback.DataCallback, Watcher {

		public void process(WatchedEvent event) {
			Event.EventType type=event.getType();
			Event.KeeperState stat=event.getState();

                        String prompt = null;
                        if(type == Event.EventType.NodeCreated || type == Event.EventType.NodeDataChanged
                                        || (type == Event.EventType.None && stat == Event.KeeperState.SyncConnected)){
                                if( type == Event.EventType.NodeCreated){
                                        prompt = "type is Event.EventType.NodeCreated";
                                }
                                else if( type == Event.EventType.NodeDataChanged ){
                                        prompt = "type is Event.EventType.NodeDataChanged";
                                }
                                else if( (type == Event.EventType.None && stat == Event.KeeperState.SyncConnected) ){
                                        prompt = "type is Event.EventType.None and stat is Event.KeeperState.SyncConnected";
                                }

                                LOG.debug("nnc, called Watcher.process(WatcheEvent), " + prompt + ", will call zk.getData(znode+ /master, this, this, null)");
                                zk.getData(znode+"/master", this, this, null);
                        }else if(type == Event.EventType.NodeDeleted ){
                                prompt = "type is Event.EventType.NodeDeleted";
                                LOG.debug("nnc, called Watcher.process(WatcheEvent), " + prompt + ", will set masterAvailable to false and call zk.exists(znode+/master, this, this, null)");
                                masterAvailable=false;
                                zk.exists(znode+"/master", this, this, null);
                        }else if(type == Event.EventType.None && stat == Event.KeeperState.Expired){
                                 prompt = "type is Event.EventType.None and stat is Event.KeeperState.Expired";
                                LOG.debug("nnc, called Watcher.process(WatcheEvent), " + prompt + ", will set masterAvailable to false");
                                masterAvailable=false;
                        }
		}

		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			if(rc == 0){
			    LOG.debug("nnc, called AsyncCallback.StatCallback.processResult(...), will call zk.getData(path, this, this, null)");
			    zk.getData(path, this, this, null);
			}else{
			    LOG.debug("nnc, called AsyncCallback.StatCallback.processResult(...), will set masterAvailable to false");
			    masterAvailable=false;
			}
		}

		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data,
				Stat stat) {
			if(rc==0 &&  data!=null && data.length > 0 ){
				try {
					nnName=new String(data);
					nnAddr = NetUtils.createSocketAddr(nnName);
					nnrpc=newRPCProxy(nnAddr);
					nn=newProxy(nnrpc);
					synchronized(this){
						masterAvailable=true;
						this.notifyAll();
					}
					LOG.debug("nnc, called AsyncCallback.DataCallback.processResult(...), got new namenode addr: " +  nnAddr.getAddress().getHostAddress());
				} catch (IOException e) {
					LOG.warn("Fail to connect the proxy",e);
					nnName=null;
					nnAddr=null;
					nn=null;
					masterAvailable=false;
					return;
				}
			}else{
				LOG.debug("nnc, called AsyncCallback.DataCallback.processResult(...), ??? set masterAvailable to false");
				masterAvailable=false;
			}
		}
		
	}
}