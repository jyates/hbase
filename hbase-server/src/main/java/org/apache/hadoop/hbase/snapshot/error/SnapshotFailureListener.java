package org.apache.hadoop.hbase.snapshot.error;

import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Listen for snapshot failures on a server
 */
public interface SnapshotFailureListener {

	  /**
	   * Notification that a given snapshot failed because of an error on the local
	   * machine
	   * @param snapshot snapshot that failed
	   * @param description explanation of why the snapshot failed
	   */
	public void snapshotFailure(String reason, SnapshotDescriptor snapshot);
	
}
