/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.tools;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 *
 */
public class ZKWatcher implements Watcher {

	/////////////////////////////////////////////////////////////////////////
	// Configuration Keys, will move to somewhere.
	/////////////////////////////////////////////////////////////////////////
	private static final String MASTER_KEY = "/master";
	private static final String SLAVES_KEY = "/slaves";
	
	// configuration: comma separated server descriptions, such as "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" 
	static final String ZK_SERVERS_KEY = "dfs.zookeeper.servers";
	static final String ZK_SERVERS_DEFAULT = "localhost:2181";
	// configuration: zk root path of nnc
	static final String ZK_ROOT_KEY = "dfs.zookeeper.path";
	static final String ZK_ROOT_DEFAULT = "/nnc";
	// configuration: zk session timeout in millisecond
	static final String ZK_TIMEOUT_KEY = "dfs.zookeeper.timeout.ms";
	static final int ZK_TIMEOUT_DEFAULT = 10000;

	/////////////////////////////////////////////////////////////////////////
	// znode path configuration utils, might be move to somewhere
	/////////////////////////////////////////////////////////////////////////
	static String getRootPath(Configuration conf) {
		return conf.get(ZK_ROOT_KEY, ZK_ROOT_DEFAULT);
	}
	
	static String getMasterPath(Configuration conf) {
		String rootPath = getRootPath(conf);
		return rootPath != null ? rootPath + MASTER_KEY : MASTER_KEY; 
	}
	
	static String getSlavesPath(Configuration conf) { 
		String rootPath = getRootPath(conf);
		return rootPath != null ? rootPath + SLAVES_KEY : SLAVES_KEY; 
	}
	
	/////////////////////////////////////////////////////////////////////////
	// ZooKeeper Conn stuff
	/////////////////////////////////////////////////////////////////////////
	ZooKeeper zk = null;
	String connectString = null;
	int zkTimeout = ZK_TIMEOUT_DEFAULT;

	private Map<String, ZKListener> listeners = null;
	private Map<String, ZKChildListener> childListeners = null;
	
	public ZKWatcher(Configuration conf) throws IOException {

		if ( conf == null ) 
			conf = new Configuration();
		
		this.connectString =  conf.get(ZK_SERVERS_KEY, ZK_SERVERS_DEFAULT);
		this.zkTimeout = conf.getInt(ZK_TIMEOUT_KEY, ZK_TIMEOUT_DEFAULT);
		
        zk = new ZooKeeper(connectString, zkTimeout, this);
        
        if (zk != null) {
        	this.prepareParentNodes(conf);
        }
            
	}
	
	synchronized void addListener(String znode, ZKListener listener) {
		
		if (listeners == null) {
			listeners = new HashMap<String, ZKListener>(4);
		}
		if ((znode != null) && (listener != null)) {
			listeners.put(znode, listener);
		}
		
		try {
			Stat stat = zk.exists(znode, true);
			if (stat == null) {
				listener.znodeDeleted();
			} else {
				String val = new String(zk.getData(znode, false, stat));
				listener.znodeCreated(val);
			}
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	synchronized void addChildListener(String znode, ZKChildListener listener) {
		if (childListeners == null) {
			childListeners = new HashMap<String, ZKChildListener>(4);
		}
		if ((znode != null) && (listener != null)) {
			childListeners.put(znode, listener);
		}
		
		try {
			Stat stat = zk.exists(znode, false);
			if( stat != null ) {
				List<String> children = zk.getChildren(znode, true);
				listener.znodeChildrenChanged(children);
			}
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	private void prepareParentNodes(Configuration conf){
		prepareNode(zk, getRootPath(conf));
		prepareNode(zk, getSlavesPath(conf));
	}
	
	static void prepareNode(ZooKeeper zk, String znode) {
		if((zk == null) || (znode == null)) {
			return;
		}
		
		String[] path = znode.split("/");
		StringBuilder psb = new StringBuilder();
		try {
			for (int i = 0; i < path.length; i++) {
				psb.append('/');
				psb.append(path[i]);
				String node = psb.toString();
				if( zk.exists(node, false) == null) {
					zk.create(psb.toString(), new byte[0], Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT);
				}
			}
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	boolean competeNode(String znode, String content) {
		try {
			zk.create(znode, content.getBytes(), Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL);
			
			String data = new String(zk.getData(znode, false, null));
			
            if ( data != null && data.compareTo(content) == 0 ) {
                    return true;
            } else {
                    return false;
            }
		} catch (KeeperException e) {
			// TODO log it
			return false;
		} catch (InterruptedException e) {
			// TODO log it
			return false;
		}
	}

	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		
        String path = event.getPath();
        if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the
            // connection has changed
            switch (event.getState()) {
            case SyncConnected:
                break;
            case Disconnected:
            	//TODO: safemode?
            	break;
            case Expired:
        //        dead = true;
                break;
            }
        } else {
        	//
        	if(path == null) {
        		return;
        	}

			try {
				if ((listeners != null) && listeners.containsKey(path)) {
					if ((event.getType() == Event.EventType.NodeCreated)||(event.getType() == Event.EventType.NodeDeleted)) {
						if( zk.exists(path, true) == null ) {
							listeners.get(path).znodeDeleted();
						} else {
							String content = new String(
									zk.getData(path, false, null));
							listeners.get(path).znodeCreated(content);
						}
					}
				} else if ((childListeners != null)
						&& childListeners.containsKey(path)) {
					List<String> children = zk.getChildren(path, true);
					childListeners.get(path).znodeChildrenChanged(children);
				}
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

        }
	}
	
	static interface ZKListener {
		
		void znodeDeleted();
		
		void znodeCreated(String content);
		
	}
	
	static interface ZKChildListener {
		
		void znodeChildrenChanged(List<String> children);
	}

}
