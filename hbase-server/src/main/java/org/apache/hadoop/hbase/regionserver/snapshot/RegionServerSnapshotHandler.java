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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.DaemonThreadFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.snapshot.monitor.FailureMonitorFactory;
import org.apache.hadoop.hbase.regionserver.snapshot.monitor.SnapshotErrorPropagator;
import org.apache.hadoop.hbase.regionserver.snapshot.monitor.SnapshotFailureListener;
import org.apache.hadoop.hbase.regionserver.snapshot.monitor.SnapshotFaultInjector;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

//XXX this is still kind of a giant mess..
/*
 * Here is what should happen (with errors)
 * 1. If we get a notification from ZK that a snapshot failed, it should just propagate to all the listener (all running snapshots)
 * 2. Those snapshots should then tell the handler to fail on ZK, if they care about that snapshot
 * 3. An internal error in creating a snapshot should follow the same path as above, causing a failure write in ZK
 * 4. (2) and (3) are fine because we have idempotent writes into ZK for snapshot failure, in fact, that means we can have this also be a listener on itself, and quickly fail a snapshot  
 */
/**
 * Handle all the detail work of dealing with snapshots for a
 * {@link HRegionServer}.
 * <p>
 * If one of the snapshots indicates that we should abort, all the current
 * snapshots are failed and we require rolling the cluster to make snapshots
 * available again (something has gone really wrong).
 * <p>
 * On startup, requires {@link #start()} to be called.
 * <p>
 * On shutdown, requires {@link #close()} to be called
 */
public class RegionServerSnapshotHandler extends Configured implements SnapshotFailureListener,
    SnapshotListener, Abortable, Closeable {
  private static final Log LOG = LogFactory.getLog(RegionServerSnapshotHandler.class);

  /** Keep threads alive in request pool for max of 60 seconds */
  public static final long DEFAULT_SNAPSHOT_THREAD_KEEP_ALIVE = 60;
  /**
   * Parallelization factor when starting snapshots on region servers. Should be
   * close to the number of regions per server.
   */
  public static final int DEFAULT_SNAPSHOT_REQUEST_THREADS = 10;
  /** Default timeout for a snapshot = 30s */
  public static final long DEFAULT_SNAPSHOT_TIMEOUT = 30000;
  /** Default amount of time to check to regions to finish a snapshot */
  private static final long DEFAULT_WAKE_FREQUENCY = 50;

  /** Controls updates for cluster <-> rs for communication in snapshots */
  private RegionZKSnapshotController snapshotZKController;

  /** Create request handlers when we get a snapshot request */
  private SnapshotRequestHandler.Factory requestHandlerFactory;

  private final RegionServerServices parent;

  /** Map of the currently running snapshot requests on the server */
  private final Map<SnapshotDescriptor, SnapshotRequestHandler> requests = new TreeMap<SnapshotDescriptor, SnapshotRequestHandler>();

  /** Map of listeners for a given snapshot failure */
  // private final List<SnapshotFailureListener> snapshotFailureListeners = new
  // ArrayList<SnapshotFailureListener>();

  private final SnapshotErrorPropagator errorPropagator = new SnapshotErrorPropagator();

  private FailureMonitorFactory failureMonitorFactory;

  public RegionServerSnapshotHandler(Configuration conf, ZooKeeperWatcher zkw,
      RegionServerServices parent) throws KeeperException {
    super(conf);
    this.parent = parent;

    // create the zk watcher
    snapshotZKController = new RegionZKSnapshotController(zkw, this, parent, this);
    // and have it always listen for failures that we care about
    this.errorPropagator.listenForSnapshotFailure(snapshotZKController);
    // and if a snapshot fails, we want to know about it to cleanup the snapshot
    this.errorPropagator.listenForSnapshotFailure(this);

  }

  /**
   * Must be called before the snapshot handler is actually used
   */
  public void start() {
    try {
      // get ready to handle snapshot requests
      setupRequestHandler();
      
      // start monitoring zk. Must be completely setup before this point
      snapshotZKController.start();
    } catch (KeeperException e) {
      abort("Failed to start watcher", e);
    }
  }

  private void setupRequestHandler(){
    // read in the snapshot request configuration properties
    long maxSnapshotWaitTime = getConf().getLong(HConstants.SNAPSHOT_TIMEOUT_KEY,
      DEFAULT_SNAPSHOT_TIMEOUT);
    long wakeFrequency = getConf().getLong(HConstants.SNAPSHOT_REQUEST_WAKE_FREQUENCY_KEY,
      DEFAULT_WAKE_FREQUENCY);
    long maxSnapshotKeepAlive = getConf().getLong(
      HConstants.SNAPSHOT_THEAD_POOL_KEEP_ALIVE_SECONDS, DEFAULT_SNAPSHOT_THREAD_KEEP_ALIVE);
    int maxSnapshotThreads = getConf().getInt(HConstants.SNAPSHOT_REQUEST_THREADS,
      DEFAULT_SNAPSHOT_REQUEST_THREADS);
    if (maxSnapshotThreads <= 1) {
      LOG.debug("Need at least two threads to run a snapshot, upping the max value to 2");
      maxSnapshotThreads = 2;
    }

    // create the snapshot requester and associated pool
    // XXX - do we need to define our own custom thread factory?
    ThreadPoolExecutor service;
    service = new ThreadPoolExecutor(1, maxSnapshotThreads, maxSnapshotKeepAlive, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), new DaemonThreadFactory("rs("
            + this.parent.getServerName().toString() + ")-snapshot-pool"));

    // setup the request handler
    failureMonitorFactory = new FailureMonitorFactory(maxSnapshotWaitTime);
    requestHandlerFactory = new SnapshotRequestHandler.Factory(parent.getWAL(), service,
        failureMonitorFactory, wakeFrequency);
  }
  
  /**
   * Exposed for TESTING!
   * @param injector injector to add faults when running a snapshot
   */
  public void addFaultInjector(SnapshotFaultInjector injector) {
    this.failureMonitorFactory.addFaultInjector(injector);
  }

  @Override
  public void close() throws IOException {
    // stop all the pending snapshot requests
    // for all snapshots we are currently running, abort them
    String reason = "Closing the handler.";
    // fail each snapshot individually
    for (SnapshotDescriptor request : requests.keySet()) {
      this.errorPropagator.snapshotFailure(request, reason);
    }

    // remove the current snapshots
    requests.clear();

    // close any outstanding requests
    this.requestHandlerFactory.close();

    // close our zk connection
    snapshotZKController.close();
  }

  @Override
  public synchronized void startSnapshot(SnapshotDescriptor snapshot) {
    // 0. Figure out if this server should be involved

    // make sure we can take requests
    // first make sure the macro elements are available
    if (parent.isStopping() || parent.isStopped()) {
      LOG.info("Can't start snapshot on RS: " + parent.getServerName()
          + ", because stopping/stopped!");
      // return false;
      return;
    }

    // check to see if we are already running this snapshot
    if (this.requests.containsKey(snapshot)) return;

    // check to see if we have regions for the snapshot
    List<HRegion> involvedRegions;
    try {
      involvedRegions = shouldHandleNewSnapshot(snapshot);
    } catch (IOException e1) {
      LOG.error("Failed to figure out if we should handle a snapshot - "
          + "something has gone awry with the online regions.", e1);
      return;// false;
    }

    // if we aren't involved, just finish
    if (involvedRegions.size() == 0) return;

    LOG.debug("Have some regions involved in snapshot:" + involvedRegions);
    // 1. Create a snapshot request
    SnapshotRequestHandler handler = requestHandlerFactory.create(snapshot, involvedRegions,
      errorPropagator,
      this.parent);
    // 2. add the request handler to the current request
    this.requests.put(snapshot, handler);
    // 3. start the snapshot
    LOG.debug("Starting to handle request for snapshot:" + snapshot.getSnapshotNameAsString());
    if (handler.start()) {
      try {
        snapshotZKController.joinSnapshot(snapshot);
      } catch (KeeperException e) {
        this.errorPropagator.snapshotFailure(snapshot,
          "Couldn't join snapshot start barrier because:" + e.getMessage());
      }
    }
  }

  @Override
  public void finishSnapshot(String snapshotName) {
    LOG.debug("Finishing snapshot: " + snapshotName);
    // 0. check to see if we are dealing with this snapshot
    SnapshotDescriptor snapshot = getKnownSnapshotDescription(snapshotName);
    LOG.debug("finishing known snapshot:" + snapshot);
    // if we don't know about that snapshot, then we are done
    if (snapshot == null) return;

    LOG.debug("Checking handler for snapshot:" + snapshotName);
    SnapshotRequestHandler handler = requests.get(snapshot);

    // 1. wait on the handler to release the region locks
    LOG.debug("Release local snapshot barrier");
    handler.releaseSnapshotBarrier();

    // 2. update ZK that we have completed the snapshot
    try {
      LOG.debug("Notifying ZK that we finished the snapshot locally.");
      snapshotZKController.finishSnapshot(snapshot);
    } catch (KeeperException e) {
      LOG.error("Couldn't update ZK that we completed the snapshot."
          + " Region servers have already been released, so snapshot will only fail globally.");
    }
    // 3. remove the snapshot from the ones we running
    LOG.debug("Cleaning up local snapshot information.");
    cleanupSnapshot(snapshot);
  }

  @Override
  public void snapshotFailure(SnapshotDescriptor failed, String reason) {
    // remove the snapshot if we know about it
    SnapshotRequestHandler handler = this.requests.remove(failed);
    if (handler == null) {
      LOG.debug("Ignoring request to fail snapshot:" + failed);
    } else LOG.info("Removed failed snapshot" + failed);
  }

  @Override
  public synchronized void remoteSnapshotFailure(String snapshotName, String reason) {
    // 0. Figure out of server should be involved
    SnapshotDescriptor snapshot = getKnownSnapshotDescription(snapshotName);
    if (snapshot == null) return;
    // 1. Propagate the error to all the listeners (includes ZK and this)
    this.errorPropagator.snapshotFailure(snapshot, reason);
  }

  /**
   * Get the description of the snapshot with the given name
   * @param snapshotName name of the snapshot to lookup
   * @return the full descriptor for the snapshot, if we know about it,
   *         <tt>null</tt> otherwise
   */
  private SnapshotDescriptor getKnownSnapshotDescription(String snapshotName) {
    for (SnapshotDescriptor desc : requests.keySet()) {
      if (desc.getSnapshotNameAsString().equals(snapshotName)) {
        return desc;
      }
    }
    return null;
  }

  /**
   * Cleanup the snapshot from those that we are currently running
   * @param snapshot
   * @return the associated request handler, if one is present, <tt>null</tt>
   *         otherwise
   */
  private SnapshotRequestHandler cleanupSnapshot(SnapshotDescriptor snapshot) {
    SnapshotRequestHandler handler = requests.remove(snapshot);
    return handler;
  }

  /**
   * Determine if the snapshot should be handled on this server
   * @param snapshot
   * @return the list of online regions. Empty list is returned if no regions
   *         are responsible for the given snapshot.
   * @throws IOException
   */
  private List<HRegion> shouldHandleNewSnapshot(SnapshotDescriptor snapshot) throws IOException {
    byte[] table = snapshot.getTableName();
    return this.parent.getOnlineRegions(table);
  }

  @Override
  public void abort(String why, Throwable e) {
    // if we already aborted, then its likely a cascading failure/abort
    // notification and we can ignore it
    if (this.isAborted()) return;

    LOG.warn("The snapshot handler was aborted because " + why + ". Aborting all snapshots.", e);
    try {
      this.close();
    } catch (IOException e1) {
      LOG.error("Failed to cleanly close!", e1);
    }

    // and then notify the parent that we aborted
    parent.abort(why, e);
  }

  @Override
  public boolean isAborted() {
    return parent.isAborted();
  }
}
