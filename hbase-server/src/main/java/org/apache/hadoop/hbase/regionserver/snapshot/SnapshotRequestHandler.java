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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.snapshot.monitor.FailureMonitorFactory;
import org.apache.hadoop.hbase.regionserver.snapshot.monitor.RunningSnapshotFailureMonitor;
import org.apache.hadoop.hbase.regionserver.snapshot.monitor.SnapshotErrorPropagator;
import org.apache.hadoop.hbase.regionserver.snapshot.monitor.SnapshotFailureListener;
import org.apache.hadoop.hbase.regionserver.snapshot.monitor.SnapshotTimer;
import org.apache.hadoop.hbase.regionserver.snapshot.status.RegionSnapshotOperationStatus;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Handle the actual snapshot manipulations on the RegionServer for a single
 * given snapshot
 */
public class SnapshotRequestHandler {

  private static final Log LOG = LogFactory.getLog(SnapshotRequestHandler.class);

  final SnapshotDescriptor snapshot;
  private final List<HRegion> regions;

  private final HLog log;
  private final RegionServerServices rss;

  private final long wakeFrequency;
  private final ExecutorCompletionService<Void> taskPool;

  private List<RegionSnapshotOperation> ops;

  private RunningSnapshotFailureMonitor errorMonitor;

  private int count = 0;

  public SnapshotRequestHandler(SnapshotDescriptor snapshot, List<HRegion> regions, HLog log,
      RegionServerServices rss, RunningSnapshotFailureMonitor monitor,
      SnapshotFailureListener errorMonitor, long wakeFreq,
      ExecutorService pool) {
    this.rss = rss;
    this.snapshot = snapshot;
    this.regions = regions;
    this.log = log;
    this.taskPool = new ExecutorCompletionService<Void>(pool);
    this.wakeFrequency = wakeFreq;
    this.errorMonitor = monitor;
  }

  /**
   * @return <tt>true</tt> on success taking the snapshot, <tt>false</tt>
   *         otherwise
   */
  public boolean start() {
    // create the monitor we are going to pass to all the snapshots, counting
    // "now" as starting the snapshot
    long now = EnvironmentEdgeManager.currentTimeMillis();
    // create a timeout monitor so we can check for timeout issues
    SnapshotTimer timeoutMonitor = errorMonitor.getTimerErrorMonitor(now,
      this.wakeFrequency);

    // Start snapshotting all the involved regions
    RegionSnapshotOperationStatus status = new RegionSnapshotOperationStatus(regions.size());
    try {
      // 1. create an operation for each region
      this.ops = new ArrayList<RegionSnapshotOperation>(regions.size());
      for (HRegion region : regions) {
        // TODO create a failure monitor for the region
        ops.add(new RegionSnapshotOperation(snapshot, region, errorMonitor, status, wakeFrequency));
      }

      // 2. submit those operations to the region snapshot runner
      for (RegionSnapshotOperation op : ops)
        taskPool.submit(op.runner(), null);


      // 2.1 do the copy table async at the same time
      if (errorMonitor.checkForError()) return false;
      submitTask(new TableInfoCopyOperation(errorMonitor, snapshot, rss));

      // wait for all the regions to become stable (no more writes) so we can
      // put a reliable snapshot point in the WAL
      if (!status.waitForRegionsToStabilize(errorMonitor)) return false;
      
      // 3. append an edit in the WAL marking our position
      // append a marker for the snapshot to the walog
      // ensures the walog is synched to this point
      WALEdit edit = HLog.getSnapshotWALEdit();
      HRegionInfo regionInfo = new ServerRegionInfo(snapshot.getTableName());
      this.log.append(regionInfo, snapshot.getTableName(), edit, now, this.regions.get(0)
          .getTableDesc());

      // 3.1 asynchronously add reference for the WALs
      submitTask(new WALReferenceOperation(snapshot, errorMonitor, this.log, rss));

      LOG.debug("Wating for snapshot to finish.");
      // 4. Wait for the regions and the wal to complete or an error
      //wait for the regions to complete their snapshotting
      while (count > 0) {
        try {
          LOG.debug("Snapshot isn't finished.");
          if (errorMonitor.checkForError())
          {
            LOG.debug("Failure monitor noticed an error -  quitting " +
            		"without waiting for snapshot tasks to complete.");
            return false;
          }
          //wait for the next task to be completed
          taskPool.take();
          count--;
        } catch (InterruptedException e) {
          // ignore
        }
      }
      LOG.debug("Snapshot completed on regionserver.");
    } catch (RejectedExecutionException e) {
      return failAndReturn("Failing snapshot because we couldn't run a part of the snapshot", e);
    } catch (SnapshotCreationException e) {
      return  failAndReturn("Failing snapshot because got creation exception", e);
    } catch (IOException e) {
      return  failAndReturn("Failing snapshot because got general IOException", e);
    }
    timeoutMonitor.complete();
    return true;
  }
  
  /**
   * @param string
   * @param e
   * @return
   */
  private boolean failAndReturn(String msg, Exception e) {
    LOG.error(msg, e);
    this.errorMonitor.snapshotFailure(snapshot, msg);
    return false;
  }

  /**
   * Submit a task to the pool. For speed, only 1 caller of this method is
   * allowed, letting us avoid locking to increment the counter
   */
  private void submitTask(Runnable task) {
    this.taskPool.submit(task, null);
    this.count++;
  }

  /**
   * Release all the locks taken on the involved regions
   */
  public void releaseSnapshotBarrier() {
    if (ops == null) {
      LOG.debug("No regions to release from the snapshot because we didn't get a chance to create them.");
      return;
    }
    LOG.debug("Releasing local barrier for snapshot.");
    for (RegionSnapshotOperation op : ops) {
      op.finish();
    }
    LOG.debug("All regions have finished their snapshot and are now accepting writes.");
  }

  /**
   * Simple helper class that allows server-level meta edits to the WAL
   * <p>
   * These edits are NOT intended to be replayed on server failure or by the
   * standard WAL player
   */
  private static class ServerRegionInfo extends HRegionInfo {
    private static final String MOCK_INFO_NAME_PREFIX = "_$NOT_A_REGION$_";

    public ServerRegionInfo(byte[] tablename) {
      super(tablename);
    }

    @Override
    public synchronized String getEncodedName() {
      return MOCK_INFO_NAME_PREFIX + super.getEncodedName();
    }

    @Override
    public boolean isMetaRegion() {
      return false;
    }
  }

  /**
   * Simple factory to create request handlers.
   * <p>
   * Takes care of making sure we use the same pool, hlog, etc
   */
  static class Factory implements Closeable {
    private final HLog log;
    private final long wakeFrequency;
    private final ThreadPoolExecutor pool;
    private final FailureMonitorFactory errorMonitorFactory;
    private final SnapshotErrorPropagator externalMonitor;

    public Factory(HLog log, ThreadPoolExecutor pool, SnapshotErrorPropagator externalMonitor,
        FailureMonitorFactory errorMonitorFactory,
        long wakeFrequency) {
      super();
      this.log = log;
      this.wakeFrequency = wakeFrequency;
      this.pool = pool;
      this.errorMonitorFactory = errorMonitorFactory;
      this.externalMonitor = externalMonitor;
    }

    public SnapshotRequestHandler create(SnapshotDescriptor desc, List<HRegion> regions,
        RegionServerServices rss) {
      // make sure any of the external listeners start watching for failures in
      // this snapshot
      RunningSnapshotFailureMonitor monitor = errorMonitorFactory.getRunningSnapshotFailureMonitor(
        desc, externalMonitor);
      // and then create the request handler
      return new SnapshotRequestHandler(desc, regions, log, rss, monitor, externalMonitor,
          wakeFrequency, pool);
    }

    @Override
    public void close() {
      this.pool.shutdownNow();
    }
  }
}
