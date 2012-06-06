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
package org.apache.hadoop.hbase.regionserver.snapshot.status;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.snapshot.SnapshotFailureException;
import org.apache.hadoop.hbase.regionserver.snapshot.SnapshotFailureListener;
import org.apache.hadoop.hbase.regionserver.snapshot.exception.SnapshotTimeoutException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Simple object to help determine if a snapshot attempt is still valid
 */
// TODO think about having this be a failure listener, that has a failure
// listener - would help simplify the objects being passed in
public class SnapshotFailureMonitorImpl implements SnapshotFailureMonitor {

  private static final Log LOG = LogFactory.getLog(SnapshotFailureMonitorImpl.class);

  private final long startTime;
  private final long maxTime;

  private final AtomicBoolean globalFailure;
  private final AtomicBoolean snapshotFailure;

  private final SnapshotFailureListener parent;
  private final SnapshotDescriptor snapshot;

  public SnapshotFailureMonitorImpl(SnapshotDescriptor snapshot, long startTime, long maxTime,
      AtomicBoolean globalStatus, AtomicBoolean snapshotStatus, SnapshotFailureListener parent) {
    this.snapshot = snapshot;
    this.startTime = startTime;
    this.maxTime = maxTime;
    this.globalFailure = globalStatus;
    this.snapshotFailure = snapshotStatus;
    this.parent = parent;
  }

  @Override
  public void checkFailure() throws SnapshotCreationException {
    if (!validTime()) throw new SnapshotTimeoutException(maxTime, timeLapse());
    if (globalFailure.get()) {
      throw new SnapshotFailureException("Recived global failure notification.");
    }
    if (snapshotFailure.get()) {
      throw new SnapshotFailureException("Recieved snapshot failure notification.");
    }
  }

  @Override
  public void localSnapshotFailure(SnapshotDescriptor snapshot, String description) {
    if (!shouldRespond(snapshot)) return;

    this.snapshotFailure.set(true);
    this.parent.localSnapshotFailure(snapshot, description);
  }

  /**
   * Check if we should respond to any update for the given snapshot
   * @param snapshot snapshot description for which we received and update
   * @return <tt>true</tt> if the snapshot description matches ours,
   *         <tt>false</tt> otherwise
   */
  private boolean shouldRespond(SnapshotDescriptor snapshot) {
    if (!this.snapshot.equals(snapshot)) {
      LOG.warn("Failure monitor received update for snapshot: " + snapshot
          + ", but we are monitoring snapshot:" + this.snapshot);
      return false;
    }
    return true;
  }
  /**
   * Simple helper factory to create monitor statuses for the state of a
   * snapshot
   */
  public static class SnapshotFailureMonitorFactory {
    private final long maxTime;
    private final AtomicBoolean globalFailure;
    private SnapshotFailureListener parent;

    /**
     * Create a factory for montiors that indicate snapshot failure after the
     * specified amount of time or if there is a global failure
     * @param maxTime max amount of time to wait for a snapshot to complete
     * @param globalFailure indicator for global failure
     * @param parent parent listener to propagate snapshot failure to other
     *          servers
     */
    public SnapshotFailureMonitorFactory(long maxTime, AtomicBoolean globalFailure,
        SnapshotFailureListener parent) {
      super();
      this.maxTime = maxTime;
      this.globalFailure = globalFailure;
      this.parent = parent;
    }

    /**
     * Create a status object to help report status to various parts of the
     * snapshot
     * @param snapshot snapshot that is being monitored
     * @param start time the snapshot started
     * @return status to be used when checking if a snapshot should proceed
     */
    public SnapshotFailureMonitorImpl createStatusMonitor(SnapshotDescriptor snapshot, long start) {
      return new SnapshotFailureMonitorImpl(snapshot, start, maxTime, globalFailure, new AtomicBoolean(
          false), parent);
    }
  }

  /**
   * Propagate the global failure to all the processes on this server
   * <p>
   * Does not attempt to notify the parent to avoid recursive failure updates
   * @param snapshot
   */
  public void remoteFailure(SnapshotDescriptor snapshot) {
    if (!shouldRespond(snapshot)) return;
    this.globalFailure.set(true);
    // TODO do some notification for waiting monitors
  }

}
