/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.snapshot;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.snapshot.operation.RegionSnapshotOperation;
import org.apache.hadoop.hbase.regionserver.snapshot.operation.RegionSnapshotOperationStatus;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.snapshot.monitor.FailureMonitorFactory;
import org.apache.hadoop.hbase.snapshot.monitor.RunningSnapshotFailureMonitor;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotErrorPropagator;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotFailureListener;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotTimer;

/**
 * Handle the actual snapshot manipulations on the RegionServer for a single
 * given snapshot
 * @param <T> Progress monitor for the region operation
 */
public abstract class SnapshotRequestHandler<T extends RegionSnapshotOperationStatus> {

  private static final Log LOG = LogFactory.getLog(SnapshotRequestHandler.class);

  protected final SnapshotDescriptor snapshot;
  protected final long wakeFrequency;

  protected final RegionServerServices rss;
  protected final List<HRegion> regions;
  protected List<RegionSnapshotOperation> ops;

  protected final ExecutorCompletionService<Void> taskPool;
  protected int count = 0;

  private SnapshotTimer timeoutMonitor;
  protected RunningSnapshotFailureMonitor errorMonitor;
  protected final T progressMonitor;

  public SnapshotRequestHandler(SnapshotDescriptor snapshot, List<HRegion> regions,
      RegionServerServices rss, RunningSnapshotFailureMonitor errorMonitor, long wakeFreq,
      ExecutorService pool, T progressMonitor) {
    this.rss = rss;
    this.snapshot = snapshot;
    this.regions = regions;
    this.taskPool = new ExecutorCompletionService<Void>(pool);
    this.wakeFrequency = wakeFreq;
    this.errorMonitor = errorMonitor;
    this.progressMonitor = progressMonitor;
  }

  /**
   * Start taking the snapshot
   * @return <tt>true</tt> if we have join the snapshot, <tt>false</tt>
   *         otherwise
   */
  public final boolean start() {
    // create the monitor we are going to pass to all the snapshots, counting
    // "now" as starting the snapshot
    this.timeoutMonitor = errorMonitor.getTimerErrorMonitor();
    this.timeoutMonitor.start();

    // actually do the snapshot taking
    boolean ret = startSnapshot();
    return ret;
  }

  /**
   * Start running the snapshot
   * @return <tt>true</tt> on success taking the snapshot, <tt>false</tt>
   *         otherwise
   */
  public abstract boolean startSnapshot();

  /**
   * Finish taking the snapshot. After this call, the region will precede
   * normally. The snapshot cannot timeout locally after this method returns.
   * @return <tt>true</tt> if we completed the snapshot successfully,
   *         <tt>false</tt> otherwise.
   */
  public final boolean complete() {
    // finish the snapshot
    boolean ret = this.commitSnapshot();
    // don't timeout once we are done
    timeoutMonitor.complete();
    return ret;
  }

  /**
   * Attempt to complete snapshot.
   * @return <tt>true</tt> if the snapshot has successfully completed locally,
   *         <tt>false</tt> otherwise (and internally pushes an error to the
   *         listening {@link SnapshotFailureListener}).
   */
  public abstract boolean commitSnapshot();

  /**
   * Fail the given snapshot for a specific reason
   * @param string Reason the snapshot failed
   * @param e exception causing the failure
   * @return always <tt>false</tt> for return chaining
   */
  protected boolean failAndReturn(String msg, Exception e) {
    LOG.error(msg, e);
    this.errorMonitor.snapshotFailure(snapshot, msg);
    return false;
  }

  /**
   * Submit a task to the pool. For speed, only 1 caller of this method is
   * allowed, letting us avoid locking to increment the counter
   */
  protected void submitTask(Runnable task) {
    this.taskPool.submit(task, null);
    this.count++;
  }


  /**
   * Wait all the regions to complete the snapshot.
   * @return <tt>true</tt> on success, <tt>false</tt> otherwise
   */
  protected boolean waitAndReturnDone() {
    if (this.progressMonitor.waitUntilDone(this.errorMonitor)) {
      LOG.debug("All regions have finished their snapshot and are now accepting writes.");
      return true;
    }
    LOG.error("Some region had an error, so we can't complete the snapshot.");
    return false;
  }

  /**
   * Wait for all of the currently outstanding tasks submitted via
   * {@link #submitTask(Runnable)}
   * @return <tt>true</tt> on success, <tt>false</tt> otherwise
   */
  protected boolean waitForOutstandingTasks() {
    LOG.debug("Waiting for snapshot to finish.");

    while (count > 0) {
      try {
        LOG.debug("Snapshot isn't finished.");
        if (errorMonitor.checkForError()) {
          LOG.debug("Failure monitor noticed an error -  quitting "
              + "without waiting for snapshot tasks to complete.");
          return false;
        }
        // wait for the next task to be completed
        taskPool.take();
        count--;
      } catch (InterruptedException e) {
        // ignore
      }
    }
    LOG.debug("Snapshot completed on regionserver.");
    return true;
  }

  /**
   * Simple factory to create request handlers.
   * <p>
   * Takes care of making sure we use the same pool, hlog, etc
   */
  static class Factory extends Configured implements Closeable {
    private final HLog log;
    private final long wakeFrequency;
    private final ThreadPoolExecutor pool;
    private final FailureMonitorFactory errorMonitorFactory;
    private final SnapshotErrorPropagator externalMonitor;


    public Factory(Configuration conf, HLog log, ThreadPoolExecutor pool,
        SnapshotErrorPropagator externalMonitor,
        FailureMonitorFactory errorMonitorFactory,
        long wakeFrequency) {
      super(conf);
      this.log = log;
      this.wakeFrequency = wakeFrequency;
      this.pool = pool;
      this.errorMonitorFactory = errorMonitorFactory;
      this.externalMonitor = externalMonitor;
    }

    /**
     * Create a request handler for the given snapshot
     * @param desc snapshot to create the handler
     * @param regions regions involved on this server for the snapshot
     * @param rss parent server services
     * @return {@link SnapshotRequestHandler} to run the snapshot on the callings server
     */
    public SnapshotRequestHandler<? extends RegionSnapshotOperationStatus> create(
        SnapshotDescriptor desc, List<HRegion> regions,
 RegionServerServices rss) {
      // make sure any of the external listeners start watching for failures in
      // this snapshot
      RunningSnapshotFailureMonitor monitor = errorMonitorFactory.getRunningSnapshotFailureMonitor(
        desc, externalMonitor);

      // and then create the request handler
      switch (desc.getType()) {
      case Global:
        return new GlobalSnapshotRequestHandler(desc, regions, log, rss, monitor, wakeFrequency,
            pool);
      case Timestamp:
      default:
        return new TimestampSnapshotRequestHandler(desc, regions, rss, monitor, wakeFrequency,
            pool, desc.getCreationTime());
      }
    }

    @Override
    public void close() {
      this.pool.shutdownNow();
    }
  }
}
