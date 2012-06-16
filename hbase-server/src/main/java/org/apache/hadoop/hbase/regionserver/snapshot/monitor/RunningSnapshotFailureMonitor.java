/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.snapshot.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Monitor the failure state of a running snapshot. This handles propagating
 * errors around the elements in the snapshot.
 * <p>
 * An instance from the
 * {@link FailureMonitorFactory#getRunningSnapshotFailureMonitor(SnapshotDescriptor, SnapshotFailureListener)}
 * should be passed to all the snapshot elements to ensure that the external
 * errors propagate to the running snapshot and internal errors propagate out.
 */
public abstract class RunningSnapshotFailureMonitor implements SnapshotFailureMonitor {
  private static final Log LOG = LogFactory.getLog(RunningSnapshotFailureMonitor.class);
  protected final SnapshotDescriptor snapshot;
  // dont use the error propagator so we can avoid multiple propagations
  private final SnapshotFailureListener errorListener;
  private volatile boolean failed = false;

  public RunningSnapshotFailureMonitor(SnapshotDescriptor snapshot,
      SnapshotFailureListener externalErrorListener) {
    this.snapshot = snapshot;
    this.errorListener = externalErrorListener;
  }

  @Override
  public synchronized final void snapshotFailure(SnapshotDescriptor snapshot, String description) {
    LOG.debug("Got a snapshot failure notification!");
    // if we don't care, just return
    if (failed || !this.snapshot.equals(snapshot)) return;

    LOG.debug("Propagating snapshot failure notification");
    // propagate the error down
    this.snapshotFailure(description);
    // mark this as failed
    this.failed = true;
    // make sure all the listener get invoked
    errorListener.snapshotFailure(snapshot, description);
  }

  @Override
  public boolean checkForError() {
    return this.failed;
  }

  /**
   * Notification that the snapshot has failed. Subclass should override to get
   * custom failure handling; overriding will not affect realization of failure
   * @param description reason why the snapshot failed
   */
  protected void snapshotFailure(String description) {
    LOG.debug("Failing snapshot because: " + description);
  }

  /**
   * Get a snapshot timer bound to this monitor
   * @param now current time (when the snapshot starts)
   * @param wakeFrequency how often to check if the snapshot has timed out
   * @return a {@link SnapshotTimer} to ensure the snapshot doesn't take too
   *         long
   */
  public abstract SnapshotTimer getTimerErrorMonitor(long now, long wakeFrequency);
}
