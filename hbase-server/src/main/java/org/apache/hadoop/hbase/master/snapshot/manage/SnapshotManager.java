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
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.SnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.DisabledTableSnapshotHandler;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.server.errorhandling.WeakReferencingNotificationBroadcasterSupport;
import org.apache.hadoop.hbase.server.errorhandling.notification.Notifications;
import org.apache.hadoop.hbase.server.errorhandling.notification.StopNotification;
import org.apache.hadoop.hbase.server.exceptionhandling.snapshot.SnapshotExceptionMonitorFactory;
import org.apache.hadoop.hbase.server.exceptionhandling.snapshot.SnapshotExceptionSnare;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;

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
public class SnapshotManager implements Stoppable {
  private static final Log LOG = LogFactory.getLog(SnapshotManager.class);

  // TODO - enable having multiple snapshots with multiple monitors

  private SnapshotExceptionMonitorFactory factory;
  private final WeakReferencingNotificationBroadcasterSupport dispatcher;

  private final MasterServices master;
  private SnapshotHandler handler;
  private ExecutorService pool;
  private final Path rootDir;

  private boolean stopped;

  public SnapshotManager(final MasterServices master, final ExecutorService executorService) {
    this(master, executorService, new SnapshotExceptionMonitorFactory());
  }

  public SnapshotManager(final MasterServices master, final ExecutorService executorService,
      final SnapshotExceptionMonitorFactory factory) {
    this.master = master;
    this.factory = factory;
    this.dispatcher = factory.getBroadcaster();
    this.pool = executorService;
    this.rootDir = master.getMasterFileSystem().getRootDir();
  }

  /**
   * @return <tt>true</tt> if there is a snapshot currently being taken, <tt>false</tt> otherwise
   * @throws SnapshotCreationException if the snapshot failed
   */
  public boolean isTakingSnapshot() throws SnapshotCreationException {
    return handler != null && !handler.getFinished();
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
    if (isTakingSnapshot()) {
      throw new SnapshotCreationException("Already running another snapshot:"
          + this.handler.getSnapshot(), snapshot);
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

    DisabledTableSnapshotHandler handler;
    try {
      SnapshotExceptionSnare monitor = this.factory.getNewSnapshotSnare(snapshot);
      handler = new DisabledTableSnapshotHandler(snapshot, parent, this.master, monitor);
      this.handler = handler;
      // have the handler listen for stop events
      Notifications.addNotificationListener(dispatcher, new StopNotification.Stopper(handler));
      // run the handler
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
    return this.handler;
  }

  @Override
  public void stop(String why) {
    // short circuit
    if (this.stopped) return;
    // make sure we get stop
    this.stopped = true;
    // pass the stop onto all the listeners
    new StopNotification.Builder(dispatcher).setMessage(why).setSource(this).send();
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  /**
   * Reset the state of the snapshot handler.
   * <p>
   * Exposed for TESTING.
   */
  public void resetForTesting() {
    setSnapshotHandlerForTesting(null);
  }

  /**
   * Set the handler for the current snapshot
   * <p>
   * Exposed for TESTING
   * @param handler handler the master should use
   */
  public void setSnapshotHandlerForTesting(SnapshotHandler handler) {
    this.handler = handler;
  }

  /**
   * Exposed for TESTING
   * @return get the current error broadcaster.
   */
  WeakReferencingNotificationBroadcasterSupport getExceptionBroadcasterForTesting() {
    return this.dispatcher;
  }
}