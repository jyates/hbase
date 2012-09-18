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
package org.apache.hadoop.hbase.master.snapshot;

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.server.Aborting;
import org.apache.hadoop.hbase.server.commit.distributed.controller.DistributedCommitCoordinatorController;
import org.apache.hadoop.hbase.server.commit.distributed.coordinator.DistributedThreePhaseCommitCoordinator;
import org.apache.hadoop.hbase.server.commit.distributed.zookeeper.ZKTwoPhaseCommitCoordinatorController;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionOrchestrator;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorListener;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorMonitorFactory;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;
import org.apache.zookeeper.KeeperException;

/**
 * This class monitors the whole process of snapshots via ZooKeeper. There is only one
 * SnapshotMonitor for the master.
 * <p>
 * Start monitoring a snapshot by calling method monitor() before the snapshot is started across the
 * cluster via a {@link DistributedCommitCoordinatorController}. SnapshotMonitor would stop
 * monitoring this snapshot only if it is finished or aborted.
 * <p>
 * Note: There could be only one snapshot being processed and monitored at a time over the cluster.
 * Start monitoring a snapshot only when the previous one reaches an end status.
 */
@InterfaceAudience.Private
public class SnapshotManager extends Aborting implements Closeable {
  private static final Log LOG = LogFactory.getLog(SnapshotManager.class);

  /** By default, check to see if the snapshot is complete (ms) */
  public static final int DEFAULT_MAX_WAIT_FREQUENCY = 500;

  /** Conf key for frequency to check for snapshot error while waiting for completion */
  public static final String MASTER_MAX_WAKE_FREQUENCY_KEY = "hbase.snapshot.master.wakeFrequency";

  /** By default, check to see if the snapshot is complete (ms) */
  public static final int DEFAULT_MAX_THREAD_KEEP_ALIVE = 5000;

  /** Conf key for frequency to check for snapshot error while waiting for completion */
  public static final String MASTER_MAX_THREAD_KEEP_ALIVE_TIME_KEY = "hbase.snapshot.master.wakeFrequency";

  // TODO - enable having multiple snapshots with multiple monitors/threads
  // this needs to be configuration based when running multiple snapshots is implemented
  /** number of current operations running on the master */
  private static final int opThreads = 1;

  private final long wakeFrequency;
  private final MasterServices master;
  private final SnapshotErrorMonitorFactory errorFactory;
  private final ExceptionOrchestrator<HBaseSnapshotException> dispatcher;
  private final DistributedThreePhaseCommitCoordinator coordinator;
  private TableSnapshotHandler handler;

  private DistributedCommitCoordinatorController controller;

  /**
   * Create a server manager with a {@link ZKTwoPhaseCommitCoordinatorController} as the
   * {@link DistributedCommitCoordinatorController}.
   * @param master parent hosting <tt>this</tt>
   * @throws KeeperException if we can't reach the zookeeper cluster
   */
  public SnapshotManager(final MasterServices master) throws KeeperException {
    this(master, new ZKTwoPhaseCommitCoordinatorController(master.getZooKeeper(),
        HConstants.ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION, master.getServerName().toString()));
  }

  public SnapshotManager(final MasterServices master,
      DistributedCommitCoordinatorController controller) throws KeeperException {
    this.master = master;

    // setup the error handling
    this.errorFactory = new SnapshotErrorMonitorFactory();
    this.dispatcher = errorFactory.getHub();

    // get the configuration for the coordinator
    Configuration conf = master.getConfiguration();
    this.wakeFrequency = conf.getInt(MASTER_MAX_WAKE_FREQUENCY_KEY, DEFAULT_MAX_WAIT_FREQUENCY);
    long keepAliveTime = conf.getLong(MASTER_MAX_THREAD_KEEP_ALIVE_TIME_KEY,
      DEFAULT_MAX_THREAD_KEEP_ALIVE);

    this.coordinator = new DistributedThreePhaseCommitCoordinator(
        master.getServerName().toString(), keepAliveTime, opThreads, wakeFrequency, controller,
        null);
    this.controller = controller;
  }

  /**
   * Start running the manager to manage new snapshots
   */
  public void start() {
    this.controller.start(coordinator);
  }

  @Override
  public void close() {
    try {
      this.controller.close();
    } catch (IOException e) {
      LOG.error("Failed to close snapshot controller.", e);
    }
    this.coordinator.close();
  }

  /**
   * Fully specify all necessary components of a snapshot manager Exposed for testing.
   * @param master services for the master where the manager is running
   * @param coordinator coordinator to coordinate online, distributed three phase commit based
   *          snapshots
   * @param monitorFactory factory to create error monitors for running snapshots
   */
  public SnapshotManager(final MasterServices master,
      DistributedThreePhaseCommitCoordinator coordinator, SnapshotErrorMonitorFactory monitorFactory) {
    this.master = master;

    this.wakeFrequency = master.getConfiguration().getInt(MASTER_MAX_WAKE_FREQUENCY_KEY,
      DEFAULT_MAX_WAIT_FREQUENCY);
    this.coordinator = coordinator;
    this.errorFactory = monitorFactory;
    this.dispatcher = errorFactory.getHub();
  }

  /**
   * @return <tt>true</tt> if there is a snapshot in process, <tt>false</tt> otherwise
   * @throws SnapshotCreationException if the snapshot failed
   */
  public boolean isInProcess() throws SnapshotCreationException {
    return handler != null && !handler.getFinished();
  }

  public synchronized OnlineTableSnapshotHandler newOnlineTableSnasphotHandler(
      SnapshotDescription snapshot, Server parent) throws IOException {
    // create a monitor for snapshot errors
    SnapshotErrorListener monitor = this.errorFactory.createMonitorForSnapshot(snapshot);

    OnlineTableSnapshotHandler handler = new OnlineTableSnapshotHandler(snapshot, parent,
        this.master, monitor, this, wakeFrequency);
    this.handler = handler;
    return handler;
  }

  public synchronized DisabledTableSnapshotHandler newDisabledTableSnasphotHandler(
      SnapshotDescription snapshot, Server parent) throws IOException {
    // Reset the snapshot to be the disabled snapshot
    snapshot = snapshot.toBuilder().setType(Type.DISABLED).build();
    // create a monitor for snapshot errors
    SnapshotErrorListener monitor = this.errorFactory.createMonitorForSnapshot(snapshot);
    DisabledTableSnapshotHandler handler = new DisabledTableSnapshotHandler(snapshot, parent,
        this.master, monitor, this);
    this.handler = handler;
    return handler;
  }

  /**
   * @return SnapshotTracker for current snapshot
   */
  public TableSnapshotHandler getCurrentSnapshotMonitor() {
    return this.handler;
  }

  @Override
  public void abort(String why, Throwable e) {
    // short circuit
    if (this.isAborted()) return;
    // make sure we get aborted
    super.abort(why, e);
    // pass the abort onto all the listeners
    this.dispatcher.receiveError(why, new HBaseSnapshotException(e));
  }

  /**
   * Reset the manager to allow another snapshot to precede
   * @param snapshotDir final path of the snapshot
   * @param workingDir directory where the in progress snapshot was built
   * @param fs {@link FileSystem} where the snapshot was built
   * @throws SnapshotCreationException if the snapshot could not be moved
   * @throws IOException the filesystem could not be reached
   */
  public void completeSnapshot(Path snapshotDir, Path workingDir, FileSystem fs)
      throws SnapshotCreationException, IOException {
    if (this.handler == null) return;
    LOG.debug("Setting handler to finsihed.");
    this.handler.finish();
    LOG.debug("Sentinel is done, just moving the snapshot from " + workingDir + " to "
        + snapshotDir);
    if (!fs.rename(workingDir, snapshotDir)) {
      throw new SnapshotCreationException("Failed to move working directory(" + workingDir
          + ") to completed directory(" + snapshotDir + ").");
    }
  }

  /**
   * Reset the state of the manager.
   * <p>
   * Exposed for TESTING.
   */
  public void reset() {
    setSnapshotHandler(null);
  }

  /**
   * Set the handler for the current snapshot
   * <p>
   * Exposed for TESTING
   * @param handler handler the master should use
   */
  public void setSnapshotHandler(TableSnapshotHandler handler) {
    this.handler = handler;
  }

  /**
   * EXPOSED FOR TESTING.
   * @return the {@link ExceptionOrchestrator} that updates all running {@link TableSnapshotHandler}
   *         in the even of an abort.
   */
  ExceptionOrchestrator<HBaseSnapshotException> getExceptionOrchestrator() {
    return this.dispatcher;
  }

  /**
   * EXPOSED FOR TESTING
   * @return distributed commit coordinator for all running snapshots
   */
  DistributedThreePhaseCommitCoordinator getCoordinator() {
    return coordinator;
  }
}