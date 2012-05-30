package org.apache.hadoop.hbase.client;

import java.util.UUID;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * An {@link OperationWithAttributes} that is tied to a specific cluste
 */
public abstract class ClusterOperation extends OperationWithAttributes {
  // Attribute used in Mutations to indicate the originating cluster.
  private static final String CLUSTER_ID_ATTR = "_c.id_";

  /**
   * Set the replication custer id.
   * 
   * @param clusterId
   */
  public void setClusterId(UUID clusterId) {
    byte[] val = new byte[2 * Bytes.SIZEOF_LONG];
    Bytes.putLong(val, 0, clusterId.getMostSignificantBits());
    Bytes.putLong(val, Bytes.SIZEOF_LONG, clusterId.getLeastSignificantBits());
    setAttribute(CLUSTER_ID_ATTR, val);
  }

  /**
   * @return The replication cluster id.
   */
  public UUID getClusterId() {
    byte[] attr = getAttribute(CLUSTER_ID_ATTR);
    if (attr == null) {
      return HConstants.DEFAULT_CLUSTER_ID;
    }
    return new UUID(Bytes.toLong(attr, 0),
        Bytes.toLong(attr, Bytes.SIZEOF_LONG));
  }

}
