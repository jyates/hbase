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
package org.apache.hadoop.hbase.master.snapshot.manage;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.snapshot.DisabledTableSnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.TableSnapshotHandler;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.server.Aborting;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionOrchestrator;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorListener;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorMonitorFactory;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotFailureListener;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * This class monitors the whole process of snapshots via ZooKeeper. There is only one
 * SnapshotMonitor for the master.
 * <p>
 * Start monitoring a snapshot by calling method monitor() before the snapshot is started across the
 * cluster via ZooKeeper. SnapshotMonitor would stop monitoring this snapshot only if it is finished
 * or aborted.
 * <p>
 * Note: There could be only one snapshot being processed and monitored at a time over the cluster.
 * Start monitoring a snapshot only when the previous one reaches an end status.
 */
@InterfaceAudience.Private
public class SnapshotManager extends Aborting {
  private static final Log LOG = LogFactory.getLog(SnapshotManager.class);

  // TODO - enable having multiple snapshots with multiple monitors

  private final MasterServices master;
  private SnapshotErrorMonitorFactory errorFactory;
  private final ExceptionOrchestrator<HBaseSnapshotException> dispatcher;
  private TableSnapshotHandler handler;

  public SnapshotManager(final MasterServices master, final ZooKeeperWatcher watcher)
      throws KeeperException {
    this.master = master;
    this.errorFactory = new SnapshotErrorMonitorFactory();
    this.dispatcher = errorFactory.getHub();
  }

  /**
   * @return <tt>true</tt> if there is a snapshot in process, <tt>false</tt> otherwise
   * @throws SnapshotCreationException if the snapshot failed
   */
  public boolean isInProcess() throws SnapshotCreationException {
    return handler != null && !handler.getFinished();
  }

  public synchronized DisabledTableSnapshotHandler newDisabledTableSnapshotHandler(
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
   *         in the even of a n abort.
   */
  ExceptionOrchestrator<HBaseSnapshotException> getExceptionOrchestrator() {
    return this.dispatcher;
  }
}