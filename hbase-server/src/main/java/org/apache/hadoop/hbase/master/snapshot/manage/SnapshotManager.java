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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.SnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.DisabledTableSnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.MasterSnapshotVerifier;
import org.apache.hadoop.hbase.master.snapshot.CloneSnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.RestoreSnapshotHandler;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionOrchestrator;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorListener;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorMonitorFactory;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
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
@InterfaceStability.Unstable
public class SnapshotManager {
  private static final Log LOG = LogFactory.getLog(SnapshotManager.class);

  public class OperationStatus {
    private Throwable error = null;
    private boolean done = false;

    public OperationStatus() {
    }

    public void completed() {
      done = true;
    }

    public void failed(final Throwable e) {
      done = true;
      error = e;
    }

    public boolean isDone() {
      return done;
    }

    public boolean isFailed() {
      return error != null;
    }

    public Throwable getError() {
      return error;
    }
  }

  private final Map<SnapshotDescription, OperationStatus> pendingRestores =
    new HashMap<SnapshotDescription, OperationStatus>();

  // TODO - enable having multiple snapshots with multiple monitors

  private final MasterServices master;
  private SnapshotErrorMonitorFactory errorFactory;
  private final ExceptionOrchestrator<HBaseSnapshotException> dispatcher;
  private volatile boolean snapshotAborted;
  private SnapshotHandler snapshotHandler;
  private ExecutorService pool;
  private final Path rootDir;

  public SnapshotManager(final MasterServices master, final ZooKeeperWatcher watcher,
      final ExecutorService executorService) throws KeeperException {
    this.master = master;
    this.errorFactory = new SnapshotErrorMonitorFactory();
    this.dispatcher = errorFactory.getHub();
    this.pool = executorService;
    this.rootDir = master.getMasterFileSystem().getRootDir();
  }

  /**
   * @return <tt>true</tt> if there is a snapshot in process, <tt>false</tt> otherwise
   * @throws SnapshotCreationException if the snapshot failed
   */
  public boolean isSnapshotInProcess() throws SnapshotCreationException {
    return snapshotHandler != null && !snapshotHandler.getFinished();
  }

  public boolean isSnapshotAborted() {
    return this.snapshotAborted;
  }

  /**
   * Check to make sure that we are OK to run the passed snapshot. Checks to make sure that we
   * aren't already running a snapshot.
   * @param snapshot description of the snapshot we want to start
   * @throws HBaseSnapshotException if the filesystem could not be prepared to start the snapshot
   */
  private synchronized void prepareToTakeSnapshot(SnapshotDescription snapshot)
      throws HBaseSnapshotException {
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir);

    // make sure we aren't already running a snapshot
    if (isSnapshotInProcess()) {
      throw new SnapshotCreationException("Already running another snapshot:"
          + this.snapshotHandler.getSnapshot());
    }

    try {
      // delete the woring directory, since we aren't running the snapshot
      fs.delete(workingDir, true);

      // recreate the working directory for the snapshot
      if (!fs.mkdirs(workingDir)) {
        throw new SnapshotCreationException("Couldn't create working directory (" + workingDir
            + ") for snapshot.", snapshot);
      }
    } catch (HBaseSnapshotException e) {
      throw e;
    } catch (IOException e) {
      throw new SnapshotCreationException(
          "Exception while checking to see if snapshot could be started.", e, snapshot);
    }
  }

  /**
   * Take a snapshot of a disabled table.
   * <p>
   * Ensures the snapshot won't be started if there is another snapshot already running. Does
   * <b>not</b> check to see if another snapshot of the same name already exists.
   * @param snapshot description of the snapshot to take. Modified to be {@link Type#DISABLED}.
   * @param parent server where the snapshot is being run
   * @throws HBaseSnapshotException if the snapshot could not be started
   */
  public synchronized void snapshotDisabledTable(SnapshotDescription snapshot, Server parent)
      throws HBaseSnapshotException {
    // setup the snapshot
    prepareToTakeSnapshot(snapshot);

    // set the snapshot to be a disabled snapshot, since the client doesn't know about that
    snapshot = snapshot.toBuilder().setType(Type.DISABLED).build();

    // create a monitor for snapshot errors
    SnapshotErrorListener monitor = this.errorFactory.createMonitorForSnapshot(snapshot);
    DisabledTableSnapshotHandler handler;
    try {
      handler = new DisabledTableSnapshotHandler(snapshot, parent, this.master, monitor, this);
      this.snapshotHandler = handler;
      this.pool.submit(handler);
    } catch (IOException e) {
      // cleanup the working directory
      Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir);
      try {
        if (this.master.getMasterFileSystem().getFileSystem().delete(workingDir, true)) {
          LOG.error("Couldn't delete working directory (" + workingDir + " for snapshot:"
              + snapshot);
        }
      } catch (IOException e1) {
        LOG.error("Couldn't delete working directory (" + workingDir + " for snapshot:" + snapshot);
      }
      // fail the snapshot
      throw new SnapshotCreationException("Could not build snapshot handler", e, snapshot);
    }
  }

  /**
   * @return the current handler for the snapshot
   */
  public SnapshotHandler getCurrentSnapshotHandler() {
    return this.snapshotHandler;
  }

  public void abortSnapshot(String why, Throwable e) {
    // short circuit
    if (this.isSnapshotAborted()) return;

    this.snapshotAborted = true;
    LOG.warn("Aborting because: " + why, e);

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
  public void resetSnapshot() {
    setSnapshotHandler(null);
  }

  /**
   * Set the handler for the current snapshot
   * <p>
   * Exposed for TESTING
   * @param handler handler the master should use
   */
  public void setSnapshotHandler(SnapshotHandler handler) {
    this.snapshotHandler = handler;
  }

  /**
   * EXPOSED FOR TESTING.
   * @return the {@link ExceptionOrchestrator} that updates all running {@link MasterSnapshotVerifier}
   *         in the even of a n abort.
   */
  ExceptionOrchestrator<HBaseSnapshotException> getExceptionOrchestrator() {
    return this.dispatcher;
  }

  /**
   * Create a new restore snapshot handler
   */
  public RestoreSnapshotHandler newRestoreSnapshotHandler(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor, long waitTime) throws IOException {
    RestoreSnapshotHandler handler = new RestoreSnapshotHandler(this.master, this,
      snapshot, hTableDescriptor, waitTime);
    this.pendingRestores.put(snapshot, new OperationStatus());
    return handler;
  }

  /**
   * Create a new clone from snapshot handler
   */
  public CloneSnapshotHandler newCloneSnapshotHandler(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor, long waitTime) throws IOException {
    CloneSnapshotHandler handler = new CloneSnapshotHandler(this.master, this,
      snapshot, hTableDescriptor, waitTime);
    this.pendingRestores.put(snapshot, new OperationStatus());
    return handler;
  }

  /**
   * @return True if the specified snapshot is in progress
   */
  public boolean isRestoreInProcess(final SnapshotDescription snapshot) {
    OperationStatus status = pendingRestores.get(snapshot);
    return status != null && !status.isDone();
  }

  /**
   * Remove the snapshot from the pending list. Operation done by the master
   * when the client ask if the restore is finished and the restore operation is done.
   * @return the restore operation status
   */
  public OperationStatus restoreRemove(final SnapshotDescription snapshot) {
    return this.pendingRestores.remove(snapshot);
  }

  /**
   * Called by the restore/clone handler to notify that the restore is completed
   */
  public void restoreCompleted(final SnapshotDescription snapshot) {
    OperationStatus status = this.pendingRestores.get(snapshot);
    if (status != null) status.completed();
  }

  /**
   * Called by the restore/clone handler to notify that the restore is failed
   */
  public void restoreFailed(final SnapshotDescription snapshot, final Throwable e) {
    OperationStatus status = this.pendingRestores.get(snapshot);
    if (status != null) status.failed(e);
  }
}