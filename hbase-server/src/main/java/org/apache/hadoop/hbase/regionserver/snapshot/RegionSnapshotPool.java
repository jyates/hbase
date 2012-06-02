package org.apache.hadoop.hbase.regionserver.snapshot;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.snapshot.status.RegionSnapshotStatus;
import org.apache.hadoop.hbase.regionserver.snapshot.status.SnapshotFailureMonitor;
import org.apache.hadoop.hbase.regionserver.snapshot.status.RegionSnapshotOperationStatus;
import org.apache.hadoop.hbase.regionserver.snapshot.status.SnapshotStatus;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Async request snapshots from the various regions
 * <p>
 * Currently only serves one snapshot/region-list at a time, but could easily be
 * augmented for multiple concurrent snapshots running at the same time.
 */
public class RegionSnapshotPool implements Closeable {
  static final Log LOG = LogFactory.getLog(RegionSnapshotPool.class);

  private final ThreadPoolExecutor pool;
  final SnapshotFailureListener listener;
  private final long wakeFrequency;

  public RegionSnapshotPool(ThreadPoolExecutor threads, SnapshotFailureListener listener,
      long wakeFrequency) {
    this.pool = threads;
    this.listener = listener;
    this.wakeFrequency = wakeFrequency;
  }

  @Override
  public void close() {
    this.pool.shutdown();
  }

  // TODO test running multiple snapshots at the same time
  /**
   * Asynchronously start snapshots on a list of regions.
   * <p>
   * Should be able to handle multiple snapshots simultaneously.
   * @param desc {@link SnapshotDescriptor} describing the snapshot to start on
   *          each region.
   * @param operations snapshot operation to submit to the pool. If any of the
   *          operations fail, the listener in the constructor is notified and
   *          no further operations are attempted
   * @param failureMonitor failure monitor so regions can keep track of the
   *          their failures
   * @return a monitor for the current status of the progress of each of the
   *         snapshot operations
   */
  public RegionSnapshotOperationStatus submitRegionSnapshotWork(SnapshotDescriptor desc,
      SnapshotFailureMonitor failureMonitor, List<RegionSnapshotOperation> operations) {
    // start the snapshot async on each region and get a pointer to the result
    // create the status monitor so we can track progress
    RegionSnapshotOperationStatus status = new RegionSnapshotOperationStatus(failureMonitor, desc,
        operations.size());

    // add the region operations to the pool
    for (RegionSnapshotOperation snapshot : operations) {
      // set the status monitor so we can keep track of the progress
      snapshot.setStatusMonitor(status);
      // start running the operation
      pool.submit(snapshot);
      // and pin the region status to the overall status
      status.addStatus(snapshot.getStatusMonitor());
    }

    return status;
  }

  /**
   * Async do genertic work for the snapshot
   * @param op operation to async perform
   * @return the status of the operation
   */
  public <T extends SnapshotStatus> T submitSnapshotWork(SnapshotOperation<T> op) {
    T status = op.getStatusMonitor();
    status.setFuture(pool.submit(op));
    return status;
  }
}
