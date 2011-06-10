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

package org.apache.hadoop.ipc;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.InetSocketAddress;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.tools.ZKWatcher;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.ipc.RPC.Invocation;
import org.apache.hadoop.ipc.RPC.VersionMismatch;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;

public class ZKRPC {
	private ZKRPC() {}
	
	private static final Log LOG = LogFactory.getLog(ZKRPC.class);

	private static class ZKInvoker implements InvocationHandler, ZKWatcher.ZKListener {
		
		private Client.ConnectionId remoteId = null;
		private Client client;
		private boolean isClosed = false;
		
		Class<? extends VersionedProtocol> protocol;
		UserGroupInformation ticket;
		Configuration conf;
		int rpcTimeout;
		
		private ZKWatcher zkhook;

		@Override
		public synchronized void znodeDeleted() {
			// TODO Auto-generated method stub
			this.remoteId = null;
			LOG.warn("RPC remote peer is lost, now suspend all RPC process.");
		}

		@Override
		public synchronized void znodeCreated(String content) {
			if(content == null) {
				LOG.warn("Got null string from zookeeper.");
				return;
			}
			
			LOG.info("RPC remote peer is being initialized as " + content);
			String server[] = content.split(":");
			if(server.length != 2) {
				LOG.warn("Server string is invalid");
				return;
			}
			
			InetSocketAddress addr = new InetSocketAddress(server[0],Integer.valueOf(server[1]).intValue());
			try {
				this.remoteId = Client.ConnectionId.getConnectionId(addr, protocol,
				          ticket, rpcTimeout, conf);
			} catch (IOException e) {
				LOG.warn("Could not connect to server indicated by zookeeper",e);
				this.remoteId = null;
			}
		}

		public ZKInvoker(Class<? extends VersionedProtocol> protocol,
				UserGroupInformation ticket, Configuration conf, 
				SocketFactory factory, int rpcTimeout)
				throws IOException {
			
			this.protocol = protocol;
			this.ticket = ticket;
			this.conf = conf;
			this.rpcTimeout = rpcTimeout;

			this.zkhook = new ZKWatcher(conf);
			this.client = RPC.CLIENTS.getClient(conf, factory);

			zkhook.addListener(ZKWatcher.getMasterPath(conf), this);
		}

		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			final boolean logDebug = LOG.isDebugEnabled();
			long startTime = 0;
			if (logDebug) {
				startTime = System.currentTimeMillis();
			}
			
			Client.ConnectionId rid;
			
			synchronized(this) {
				rid = remoteId;
			}

			if( rid == null ) {
				LOG.error("No RPC remote peer available.");
				throw new ConnectException("No RPC remote peer available.");
			}
			
			ObjectWritable value = (ObjectWritable) client.call(new Invocation(
					method, args), rid);
			if (logDebug) {
				long callTime = System.currentTimeMillis() - startTime;
				LOG.debug("Call: " + method.getName() + " " + callTime);
			}
			return value.get();
		}

		/* close the IPC client that's responsible for this invoker's RPCs */
		synchronized private void close() {
			if (!isClosed) {
				isClosed = true;
				RPC.CLIENTS.stopClient(client);
			}
		}
	}

	public static VersionedProtocol getProxy(
		      Class<? extends VersionedProtocol> protocol,
		      long clientVersion, InetSocketAddress addr, UserGroupInformation ticket,
		      Configuration conf, SocketFactory factory, int rpcTimeout) throws IOException {
		
	    if (UserGroupInformation.isSecurityEnabled()) {
	        SaslRpcServer.init(conf);
	    }
	    
	    VersionedProtocol proxy =
	        (VersionedProtocol) Proxy.newProxyInstance(
	            protocol.getClassLoader(), new Class[] { protocol },
	            new ZKInvoker(protocol, ticket, conf, factory, rpcTimeout)); //reimplements a handler
	    
	    long serverVersion = proxy.getProtocolVersion(protocol.getName(), 
	                                                  clientVersion);
	    if (serverVersion == clientVersion) {
	      return proxy;
	    } else {
	      throw new VersionMismatch(protocol.getName(), clientVersion, 
	                                serverVersion);
	    }
	}

    public static void stopProxy(VersionedProtocol proxy) {
		    if (proxy!=null) {
		      ((ZKInvoker)Proxy.getInvocationHandler(proxy)).close();
		    }
	}

}
