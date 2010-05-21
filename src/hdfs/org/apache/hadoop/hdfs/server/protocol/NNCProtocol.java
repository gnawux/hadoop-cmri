/**
 * 
 */
package org.apache.hadoop.hdfs.server.protocol;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * @author gnawux
 *
 */
public interface NNCProtocol extends VersionedProtocol {
	public static final long versionID = 1L;
	
	public NNSlaveInfo register(NNSlaveInfo slave) throws IOException;
	public NamespaceInfo versionRequest() throws IOException;
	
}
