package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.tools.ZKWatcher;
import org.apache.hadoop.hdfs.tools.ZKWatcher.ZKChildListener;
import org.apache.hadoop.hdfs.tools.ZKWatcher.ZKListener;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;

public class NNCPivot {
	
	private Configuration conf = null;

	private NameNode namenode = null;
	
	private ZKWatcher zk = null;
	
	private RPCHub rpchub = new RPCHub();

	public NNCPivot(Configuration conf, NameNode namenode) throws IOException {
		this.namenode = namenode;
		this.conf = conf;
		this.zk = new ZKWatcher(conf);
	}
	
	public Object getRPCBroker() {
		return rpchub;
	}

	class RPCHub implements ClientProtocol, DatanodeProtocol, NamenodeProtocol {
		
		@Override
		public long getProtocolVersion(String protocol, long clientVersion)
				throws IOException {
			return namenode.getProtocolVersion(protocol, clientVersion);
		}

		@Override
		public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
				throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ExportedBlockKeys getBlockKeys() throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public long getEditLogSize() throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public CheckpointSignature rollEditLog() throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void rollFsImage() throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public DatanodeRegistration register(DatanodeRegistration registration)
				throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public DatanodeCommand[] sendHeartbeat(
				DatanodeRegistration registration, long capacity, long dfsUsed,
				long remaining, int xmitsInProgress, int xceiverCount)
				throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public DatanodeCommand blockReport(DatanodeRegistration registration,
				long[] blocks) throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void blockReceived(DatanodeRegistration registration,
				Block[] blocks, String[] delHints) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public void errorReport(DatanodeRegistration registration,
				int errorCode, String msg) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public NamespaceInfo versionRequest() throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public UpgradeCommand processUpgradeCommand(UpgradeCommand comm)
				throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public long nextGenerationStamp(Block block, boolean fromNN)
				throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void commitBlockSynchronization(Block block,
				long newgenerationstamp, long newlength, boolean closeFile,
				boolean deleteblock, DatanodeID[] newtargets)
				throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public LocatedBlocks getBlockLocations(String src, long offset,
				long length) throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void create(String src, FsPermission masked, String clientName,
				boolean overwrite, short replication, long blockSize)
				throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public LocatedBlock append(String src, String clientName)
				throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean recoverLease(String src, String clientName)
				throws IOException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean setReplication(String src, short replication)
				throws IOException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void setPermission(String src, FsPermission permission)
				throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public void setOwner(String src, String username, String groupname)
				throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public void abandonBlock(Block b, String src, String holder)
				throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public LocatedBlock addBlock(String src, String clientName)
				throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public LocatedBlock addBlock(String src, String clientName,
				DatanodeInfo[] excludedNodes) throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean complete(String src, String clientName)
				throws IOException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public boolean rename(String src, String dst) throws IOException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean delete(String src) throws IOException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean delete(String src, boolean recursive) throws IOException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean mkdirs(String src, FsPermission masked)
				throws IOException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public DirectoryListing getListing(String src, byte[] startAfter)
				throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void renewLease(String clientName) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public long[] getStats() throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
				throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public long getPreferredBlockSize(String filename) throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public boolean setSafeMode(SafeModeAction action) throws IOException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void saveNamespace() throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public void refreshNodes() throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public void finalizeUpgrade() throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public UpgradeStatusReport distributedUpgradeProgress(
				UpgradeAction action) throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void metaSave(String filename) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public HdfsFileStatus getFileInfo(String src) throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ContentSummary getContentSummary(String path) throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void setQuota(String path, long namespaceQuota,
				long diskspaceQuota) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public void fsync(String src, String client) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public void setTimes(String src, long mtime, long atime)
				throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
				throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
				throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
				throws IOException {
			// TODO Auto-generated method stub

		}

	}

	
	class StateMaintainer implements ZKChildListener, ZKListener {
		
		void masterBootup() {
			//TODO: check and prepare zk node
			//TODO: init RPCBroker
		}
		
		void slaveBootup() {
			//TODO: zkstart... maybe lock here
			
			try {
				SecondaryNameNode snn = new SecondaryNameNode(conf);
				snn.doCheckpoint();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//TODO: release the ckpt lock in zookeeper
			
		}

		@Override
		public void znodeDeleted() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void znodeCreated(String content) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void znodeChildrenChanged(List<String> children) {
			// TODO Auto-generated method stub
			
		}
	}
	
	class UpdateHandler {
		
	}
	
	class Tranceiver {
		
	}
	
	class MetaDumper {
		
	}
	
	class Bootstrapper {
		
	}
}
