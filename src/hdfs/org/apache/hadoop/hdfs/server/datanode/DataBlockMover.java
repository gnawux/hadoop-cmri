package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.hdfs.server.protocol.VolumeManagerProtocol.FSVolumeState;
import org.apache.hadoop.util.Daemon;

public class DataBlockMover implements Runnable { 
	public static final Log LOG = LogFactory.getLog(DataBlockMover.class);
	
	DataNode datanode;
	DataStorage storage;
	FSDataset dataset;
	final long defaultCacheSize = 4 * 1024 * 1024; /* 4M bytes */ 
	final long cacheSize;
	Configuration conf;
	List<FSVolume> volumes = new LinkedList<FSVolume>();
	Daemon dataBlockMoverThread = null;
	
	DataBlockMover(DataNode datanode, 
			       DataStorage storage,
			       FSDatasetInterface dataset,
			       Configuration conf) throws IOException {
		this.datanode = datanode;
		this.storage = storage;
		if (dataset instanceof FSDataset) {
			this.dataset = (FSDataset)dataset;
		} else {
			throw new IOException("Block Mover is supported only with FSDataset.");
		}
		this.conf = conf;
		this.cacheSize = conf.getLong("dfs.datanode.mover.cache", defaultCacheSize);
		this.dataBlockMoverThread = new Daemon(this);
		this.dataBlockMoverThread.checkAccess();
		this.dataBlockMoverThread.setPriority(this.dataBlockMoverThread.getPriority() - 1);
		this.dataBlockMoverThread.start(); 
	}
	
	public boolean startMoveTask(FSVolume srcVol) { //*CR* Is this mean: If srcVol exists and is not in volumes, add it into it.
		if (srcVol != null) {
			synchronized (this.volumes) {
				if (srcVol.getVolumeState() == FSVolumeState.VOL_STATE_LOCKED) {
					Iterator<FSVolume> volIt = this.volumes.iterator();
					while (volIt.hasNext()) {
						FSVolume vol = volIt.next();
						if (srcVol.getVolumeId() == vol.getVolumeId()) {
							return false;
						}
					}
					volumes.add(srcVol);
					return true;
				}
			}
		}
		return false;
	}
	
	public void run() {
		HashMap<Block, DatanodeBlockInfo> volumeMap = null;
		long i = 0,blockCount = 0;
		int progress = 0, lastProgress = 0;
		Iterator<Entry<Block, DatanodeBlockInfo>> it;
		FSVolume srcVol = null;
		
		volumeMap = new HashMap<Block, DatanodeBlockInfo>();
		DataBlockMover.LOG.info("Block Mover is started.");
		while (datanode.shouldRun) {
			try {
				Thread.sleep(1000);
				
				synchronized (this.volumes) {
					if (this.volumes.size() == 0) {
						continue;
					}
					srcVol = this.volumes.remove(0); //*CR* How to prevent a vol is added again after it has been removed here?
				}
				if (srcVol == null) {
					continue;
				}
				// get all of the blocks
				volumeMap.clear();
				lastProgress = progress = 0;
				i = 0;
				srcVol.getVolumeMap(volumeMap);
				it = volumeMap.entrySet().iterator();
				blockCount = volumeMap.size();
				if (blockCount > 0) {
					srcVol.setVolumeState(FSVolumeState.VOL_STATE_COPYING);
					srcVol.setVolumeProgress(0);
				}
				while (it.hasNext()) { 
					Entry<Block, DatanodeBlockInfo> entry = it.next();
					Block block = entry.getKey();
					DatanodeBlockInfo dbi = (DatanodeBlockInfo)entry.getValue();
					FSVolume destVol = null;
					File dest = null;  /* it's a directory */
					
					this.dataset.writeLock(block);
					try {
						File srcBlockFile = dbi.getFile();
						File srcMetaFile = null;
						File destBlockFile = null;
						File destMetaFile = null;
						destVol = this.dataset.getDestVolume(srcVol, block.getNumBytes());
						if (destVol == null) {
							DataBlockMover.LOG.warn("Insufficient space to move.");
							break;
						}
						dest = destVol.getDir();
						try {
							destBlockFile = this.dataset.moveBlockToVolume(block, destVol);
						} catch (IOException e1) {
							continue;
						}
						destMetaFile = FSDataset.getMetaFile(destBlockFile, block);
						srcMetaFile = FSDataset.getMetaFile(srcBlockFile, block);
						// perhaps, the block has invalidated.
						if (!srcMetaFile.exists()) {
							LOG.warn("the Meta file " + srcMetaFile + " isn\'t exist.");
						} else if (!srcBlockFile.exists()) {
							LOG.warn("the Block file " + srcBlockFile + " isn\'t exist.");
						} else {
							try {
								// copy Block file
								if (!copyFile(srcBlockFile, destBlockFile, false)) {
									throw new IOException("copy file from "
											+ srcBlockFile.toString() + " to "
											+ dest.toString() + " failed.");
								}
								// copy Meta file
								if (!copyFile(srcMetaFile, destMetaFile, false)) {
									throw new IOException("copy file from "
											+ srcMetaFile.toString() + " to "
											+ dest.toString() + " failed.");
								}
							} catch (IOException e) {
								DataBlockMover.LOG.warn(e.toString());
								continue;
							}
							try {
								this.dataset.finalizeBlockMove(destVol, block,
										destBlockFile);
							} catch (IOException e) {
								continue;
							}
						}
						// count and save the progress
						++i;
						progress = (int)((i * 100) / blockCount);
						if (progress != lastProgress) {
							srcVol.setVolumeProgress(progress);
							LOG.info("Copying(" + srcVol.getVolumeProgress() + "%)" +
									" from volume " + srcVol.getVolumeId() + " to volume " +
									destVol.getVolumeId() + ".");
							lastProgress = progress;
						}
					} finally {
						this.dataset.writeUnlock(block);
					}
				}
				// copy ok, set the state pending remove mode. wait for
				// reading or writing operation finished. then remove the 
				// volume automatically.
				srcVol.setVolumeState(FSVolumeState.VOL_STATE_PENDINGRM);
				if (srcVol.getRefCount() == 0) {
					this.datanode.realDelVolume(srcVol);
				}
				// save the new volume directory table.
				this.datanode.flushConfig();
				srcVol = null;
			} catch (InterruptedException e) {
			}
			finally {
				//*CR* What's the 'finally' mean?
			}
		}
	}
	
	public void shutdown() {
		
	}
	
	private boolean copyFile(File src,File dst, boolean force) throws IOException {
		FileInputStream in = new FileInputStream(src);
		FileOutputStream out = new FileOutputStream(dst);
		FileChannel inChannel = in.getChannel();
		FileChannel outChannel = out.getChannel();
		boolean bret = false;
		
		try {
			bret = copyFile(inChannel,outChannel);
		} finally {
			if (inChannel != null) {
				inChannel.close();
				inChannel = null;
			}
			if (outChannel != null) {
				outChannel.close();
				outChannel = null;
			}
			if (in != null) {
				in.close();
				in = null;
			}
			if (out != null) {
				out.close();
				out = null;
			}
		}
		return bret;
	}
		
	private boolean copyFile(FileChannel inChannel, FileChannel outChannel) throws IOException {
		long bytesToSend = 0;
		long time = DataNode.now();
		
		while (inChannel.position() < inChannel.size()) {
			long left = inChannel.size() - inChannel.position();
			bytesToSend = (left < cacheSize) ? left : cacheSize;

			inChannel.transferTo(inChannel.position(), bytesToSend, outChannel);
			inChannel.position(inChannel.position() + bytesToSend);
		}
		time = DataNode.now() - time;
		return true;
	}
}
