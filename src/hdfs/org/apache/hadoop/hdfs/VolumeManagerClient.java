package org.apache.hadoop.hdfs;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.protocol.VolumeManagerProtocol;
import org.apache.hadoop.hdfs.server.protocol.VolumeManagerProtocol.FSVolumeInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;

public class VolumeManagerClient {
	VolumeManagerProtocol vmp;
	
	VolumeManagerClient(String remoteConn ,Configuration conf) 
	               throws IOException {
		InetSocketAddress addr = NetUtils.createSocketAddr(remoteConn);

		vmp = (VolumeManagerProtocol)RPC.getProxy(VolumeManagerProtocol.class,
					VolumeManagerProtocol.versionID, addr, conf);
	}
	
	public void addVolume(String path) {
		File root = new File(path);
		long lret = 0;
		
		if (!root.exists()) {
			System.out.println("the path " + path + " isn\'t exist,please check it.");
			return;
		}
		try {
			lret = vmp.addVolume(path);
		} catch (IOException e) {
			System.out.println(e.toString());
		}
		if (lret != 0) {
			System.out.println("add volume successfully,volume id = " + lret);
		}
	}
	
	public void delVolume(String strVid) {
		long vid = Long.valueOf(strVid);
		
		try {
			vmp.delVolume(vid);
		} catch (IOException e) {
			System.out.println(e.toString());
		}
	}
	
	public void listVolumes() {
		FSVolumeInfo[] volArray = null;
		
		try {
			volArray = vmp.listVolumes();
		} catch (IOException e) {
			System.out.println(e.toString());
		}
		printVolumes(volArray);
	}
	
	private String getSpeedString(long numBytes,long numTimeMs) {
		double Bps = (double) ((double)numBytes * 
				1000 * 1.0 / (numTimeMs));
		DecimalFormat df = new DecimalFormat("####.00");
		String strSpeed = "";
		if (Bps > 1024 && Bps < 1024*1024) {
			Bps /= 1024;
			strSpeed = df.format(Bps);
			strSpeed += "KB/s";
			return strSpeed;
		} else if (Bps > 1024*1024 && Bps < 1024*1024*1024) {
			Bps /= 1024*1024;
			strSpeed = df.format(Bps);
			strSpeed += "MB/s";
			return strSpeed;
		} else if (Bps > 1024 * 1024 * 1024 && Bps < 1024*1024*1024*1024) {
			Bps /= 1024*1024*1024;
			strSpeed = df.format(Bps);
			strSpeed += "GB/s";
			return strSpeed;
		} else {
			return "Overflow";
		}
	}
	
	private void printVolumes(FSVolumeInfo[] volArray) {
		System.out.println("vid\tstate\t\t\t\terror\tpath");
		if (volArray != null) {
			for (int i=0; i<volArray.length; i++) {
				if (volArray[i] != null) {
					System.out.print(volArray[i].getVolumeId() + "\t");
					switch(volArray[i].getVolumeState()) {
					case VOL_STATE_NORMAL:
						if (volArray[i].getWrittenBytes() != 0) {
							System.out.print("NORMAL(" + 
									getSpeedString(volArray[i].getWrittenBytes(),
											       volArray[i].getWrittenTime()) + ")\t");
						} else {
							System.out.print("NORMAL\t\t");
						}
						break;
					case VOL_STATE_COPYING:
						System.out.print("COPYING(" + volArray[i].getProgress() + "%)");
						break;
					case VOL_STATE_LOCKED:
						System.out.print("LOCKED\t");
						break;
					case VOL_STATE_PENDINGRM:
						System.out.print("PENDING_REMOVE(" + volArray[i].getRefCount() + ")");
						break;
					default:
						System.out.print("Unknown\t");
						break;
					}
					System.out.print("\t");
					System.out.print(volArray[i].getChecksumFailedCount() + "\t");
					System.out.println(volArray[i].getDataDir().toString());
				}
			}
		}
		else {
			System.out.println("no volume.");
		}
	}
	
	public static void printUsage() {
		System.out.println("-listvol");
		System.out.println("-addvol <path>");
		System.out.println("-delvol <vid>");
	}
	
	public static void main(String[] args) {
		VolumeManagerClient vmClient = null;
		Configuration conf = null;
		String remoteConn = "127.0.0.1:50082";
		
		if (args.length > 0 && args.length <= 2) {
			conf = new Configuration();
			try {
				vmClient = new VolumeManagerClient(remoteConn,conf);
			} catch (IOException e) {
				System.out.println(e.toString());
				return;
			}
			
			if (args.length == 1) {
				if (args[0].equals("-listvol")) {
					vmClient.listVolumes();
					return;
				}
			} else if (args.length == 2) {
				if (args[0].equals("-addvol")) {
					vmClient.addVolume(args[1]);
					return;
				}
				else if (args[0].equals("-delvol")) {
					vmClient.delVolume(args[1]);
					return;
				}
			}
		}
		printUsage();
		System.exit(1);
	}
}
