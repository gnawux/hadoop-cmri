package org.apache.hadoop.hdfs.server.namenode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableComparable;

public class NNSlaveID implements WritableComparable {

    protected String name;      /// hostname:portNumber
	protected String nnSlaveID; /// unique per cluster storageID
	protected int infoPort;     /// the port where the infoserver is running

	public NNSlaveID() {
		this("", "", -1);
	}
	
	public NNSlaveID(NNSlaveID from) {
		this(from.getName(), from.getNNSlaveID(), from.getInfoPort());
	}

    public NNSlaveID(String nodeName, String nnSlaveID, int infoPort) {
		this.name = nodeName;
		this.nnSlaveID = nnSlaveID;
		this.infoPort = infoPort;
	}

    public String getName() {
        return name;
    }
      
    public void setName(String nodeName){
    	this.name=nodeName;
    }
    public String getNNSlaveID() {
        return this.nnSlaveID;
    }

    public int getInfoPort() {
        return infoPort;
    }

    void setNNSlaveID(String nnSlaveID) {
        this.nnSlaveID = nnSlaveID;
      }

    public String getHost() {
        int colon = name.indexOf(":");
        if (colon < 0) {
          return name;
        } else {
          return name.substring(0, colon);
        }
    }
      
    public int getPort() {
        int colon = name.indexOf(":");
        if (colon < 0) {
          return 50010; // default port.
        }
        return Integer.parseInt(name.substring(colon+1));
    }

    public boolean equals(Object to) {
        if (this == to) {
          return true;
        }
        if (!(to instanceof NNSlaveID)) {
          return false;
        }
        return (name.equals(((NNSlaveID)to).getName()) &&
                nnSlaveID.equals(((NNSlaveID)to).getNNSlaveID()));
    }
      
      public int hashCode() {
        return name.hashCode()^ nnSlaveID.hashCode();
      }
      
      public String toString() {
        return name;
      }
      
      /**
       * Update fields when a new registration request comes in.
       * Note that this does not update nnSlaveID.
       */
      void updateRegInfo(NNSlaveID nodeReg) {
        name = nodeReg.getName();
        infoPort = nodeReg.getInfoPort();
        // update any more fields added in future.
      }
    
	@Override
	public void readFields(DataInput in) throws IOException {
	    name = Text.readString(in);
	    nnSlaveID = Text.readString(in);
	    // the infoPort read could be negative, if the port is a large number (more
	    // than 15 bits in storage size (but less than 16 bits).
	    // So chop off the first two bytes (and hence the signed bits) before 
	    // setting the field.
	    this.infoPort = in.readShort() & 0x0000ffff;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, name);
	    Text.writeString(out, nnSlaveID);
	    out.writeShort(infoPort);
	}

	@Override
	public int compareTo(Object o) {
	    return name.compareTo(((NNSlaveID)o).getName());
	}

}
