package org.apache.hadoop.hbase.snapshot.error;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.server.error.ErrorMonitor;
import org.apache.hadoop.hbase.server.error.ProcessTimer;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Utility class to make it easier to deal with creating a timer for a snapshot
 */
public class SnapshotTimerUtils {

  private SnapshotTimerUtils(){
    //hidden ctor because it is a util class
  }
  
  /**
   * Create a snapshot timer for the master which notifies the monitor when an error occurs
   * @param snapshot snapshot to monitor
   * @param conf configuration to use when getting the max snapshot life
   * @param monitor monitor to notify when the snapshot life expires
   * @return the timer to use update to signal the start and end of the snapshot
   */
  public ProcessTimer getMasterTimerAndBindToMonitor(SnapshotDescriptor snapshot, Configuration conf, ErrorMonitor<HBaseSnapshotException> monitor){
    long maxTime = snapshot.getType().getMaxMasterTimeout(conf, SnapshotDescriptor.DEFAULT_MAX_WAIT_TIME);
    return new ProcessTimer(monitor, maxTime, snapshot);
  }
  
  /**
   * Create a snapshot timer for the regionserver snapshot process which notifies the monitor when an error occurs
   * @param snapshot snapshot to monitor
   * @param conf configuration to use when getting the max snapshot life
   * @param monitor monitor to notify when the snapshot life expires
   * @return the timer to use update to signal the start and end of the snapshot
   */
  public ProcessTimer getRegionServerTimerAndBindToMonitor(SnapshotDescriptor snapshot, Configuration conf, ErrorMonitor<HBaseSnapshotException> monitor){
    long maxTime = snapshot.getType().getMaxRegionTimeout(conf, SnapshotDescriptor.DEFAULT_REGION_SNAPSHOT_TIMEOUT);
    return new ProcessTimer(monitor, maxTime, snapshot);
  }
}
