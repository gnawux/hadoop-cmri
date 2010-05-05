package org.apache.hadoop.hdfs.zookeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/*
 * Reading Load Balance NameNode
 */
public class RLBNameNode extends ZkNameNode implements DualWayNameNode{

	protected List<String>			  		names=new LinkedList<String>();
	protected Map<String,InetSocketAddress> alist=new HashMap<String,InetSocketAddress>();
	protected Map<String,VersionedProtocol> rpclist=new HashMap<String,VersionedProtocol>();
	protected Map<String,VersionedProtocol> nlist=new HashMap<String,VersionedProtocol>();

	protected ListWatcher 	listWatcher=new ListWatcher();
	protected boolean		listAvailable=false;
	
	Random r=new Random(System.currentTimeMillis());

	public RLBNameNode(Class<?> proto, long version, Configuration cf) throws IOException{
		super(proto,version,cf);
	}
	
	public RLBNameNode(Class<?> proto, long version, Configuration cf, UnixUserGroupInformation ugi, Map<String,RetryPolicy> methodNameToPolicyMap) throws IOException{
		super(proto,version,cf, ugi, methodNameToPolicyMap);
	}

	public void init() throws IOException {
		super.init();
		fetchNameNodeList();
	}
	
	public void stop(){
		super.stop();
		if(rpclist!=null)
			for(VersionedProtocol rp : rpclist.values()){
				RPC.stopProxy(rp);
			}
	}
	
	@Override
	public InetSocketAddress altAddress() throws IOException {
		if(listAvailable){
			return alist.get(names.get(nextIndex()));
		}
		
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
		return alist.get(names.get(nextIndex()));
	}

	@Override
	public VersionedProtocol altNameNode() throws IOException {
		if(listAvailable){
			return nlist.get(names.get(nextIndex()));
		}
		
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
		return nlist.get(names.get(nextIndex()));
	}

	protected void fetchNameNodeList(){
		if(listWatcher==null)
			listWatcher=new ListWatcher();
		
		listAvailable=false;
		
		zk.getChildren(znode+"/namenodes", listWatcher, listWatcher, null);
	}
	
	protected int nextIndex(){
		return r.nextInt(names.size());
	}

	protected class ListWatcher implements AsyncCallback.ChildrenCallback, Watcher {

		public void process(WatchedEvent event) {
			Event.EventType type=event.getType();
			Event.KeeperState stat=event.getState();

			if(type == Event.EventType.NodeChildrenChanged
					|| (type == Event.EventType.None && stat == Event.KeeperState.SyncConnected)){
				zk.getChildren(znode+"/namenodes", this, this, null);
			}else if(type == Event.EventType.None && stat == Event.KeeperState.Expired ){
				
			}
		}

		@Override
		public void processResult(int rc, String path, Object ctx,
				List<String> children) {
			if(rc==0){
				if(children!=null && children.size()>0){
					synchronized(this){
						for(Iterator<String> iter=children.iterator();iter.hasNext();){
							String n=iter.next();
							
							if(names.contains(n)){
								names.remove(n);
							}else{
								InetSocketAddress a=NetUtils.createSocketAddr(n);
								try {
									VersionedProtocol rp=newRPCProxy(a);
									VersionedProtocol p=newProxy(rp);
									alist.put(n, a);
									rpclist.put(n, rp);
									nlist.put(n, p);
								} catch (IOException e) {
									LOG.warn("cannot connect the proxy " + n);
									iter.remove();
								}
							}
						}
						for(String d : names){ //those not in children
							alist.remove(d);
							rpclist.remove(d);
							nlist.remove(d);
						}
						names=children;
						listAvailable=true;
						this.notifyAll();
					}
					return;
				}
			}
			listAvailable=false;
			synchronized(this){
				names.clear();
				alist.clear();
				nlist.clear();
			}
		}
	}

}