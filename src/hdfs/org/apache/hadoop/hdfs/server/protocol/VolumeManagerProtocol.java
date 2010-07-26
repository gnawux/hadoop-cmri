package org.apache.hadoop.hdfs.server.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.VersionedProtocol;

public interface VolumeManagerProtocol extends VersionedProtocol {
	  public static final long versionID = 1L;
	  
	  enum FSVolumeState {
		  VOL_STATE_NORMAL,
		  VOL_STATE_COPYING,
		  VOL_STATE_LOCKED,
		  VOL_STATE_PENDINGRM  /* pending removed */
	  }
	  
	  class FSVolumeInfo implements Writable {
		  private File root;     /* volume that belongs to folder */
		  private long vid;      /* volume id */
		  private FSVolumeState state; /* volume state */
		  private int  progress; /* progress that copying(0-100%), used for copying */
		  private int refCount; /* current read or write operation Count */
		  private long numBytesWritten; 
		  private long numWrittenTimeMs; 
		  private long numBytesRead;
		  private long numReadTimeMs;
		  private long checksumFailedErrorCount;
		  
		  public FSVolumeInfo() { }
		  
		  public FSVolumeInfo(File root, long vid) {
			  this.root = root;
			  this.vid = vid;
		  }
		  
		  public File  getDataDir()  { return root; }
		  public long  getVolumeId() { return vid;  }
		  public void  setVolumeState(FSVolumeState state) { this.state = state;}
		  public void  setVolumeProgress(int progress) { this.progress = progress;}
		  public FSVolumeState getVolumeState() { return this.state; }
		  public int getProgress() { return this.progress; }
		  public int getRefCount() { return this.refCount; }
		  public void setRefCount(int refCount) { this.refCount = refCount; }
		  public long getWrittenBytes() { return this.numBytesWritten; }
		  public long getReadBytes() { return this.numBytesRead; }
		  public long getWrittenTime() { return this.numWrittenTimeMs; }
		  public long getReadTime() { return this.numReadTimeMs; }
		  public long getChecksumFailedCount() { return this.checksumFailedErrorCount; }
		  public void setWrittenBytes(long numBytes) { this.numBytesWritten = numBytes; }
		  public void setReadBytes(long numBytes) { this.numBytesRead = numBytes; }
		  public void setWrittenTime(long timeMs) { this.numWrittenTimeMs = timeMs; }
		  public void setReadTime(long timeMs) { this.numReadTimeMs = timeMs; }
		  public void setChecksumFailedCount(long numFailed) { 
			  this.checksumFailedErrorCount =  numFailed;
		  }

		  public void readFields(DataInput in) throws IOException {
			  root = new File(UTF8.readString(in));
			  vid = in.readLong();
			  state = FSVolumeState.valueOf(UTF8.readString(in));
			  progress = in.readInt();
			  refCount = in.readInt();
			  numBytesWritten = in.readLong();
			  numWrittenTimeMs = in.readLong();
			  numBytesRead = in.readLong();
			  numReadTimeMs = in.readLong();
			  checksumFailedErrorCount = in.readLong();
		  }

		  public void write(DataOutput out) throws IOException {
			  UTF8.writeString(out, root.toString());
			  out.writeLong(vid);
			  UTF8.writeString(out, state.toString());
			  out.writeInt(progress);
			  out.writeInt(refCount);
			  out.writeLong(numBytesWritten);
			  out.writeLong(numWrittenTimeMs);
			  out.writeLong(numBytesRead);
			  out.writeLong(numReadTimeMs);
			  out.writeLong(checksumFailedErrorCount);
		  }
	  }
	  
	  public long addVolume(String root) throws IOException;
	  public void delVolume(long vid) throws IOException;
	  public FSVolumeInfo[] listVolumes() throws IOException;

}
