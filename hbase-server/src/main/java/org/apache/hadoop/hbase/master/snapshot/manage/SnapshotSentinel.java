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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.snapshot.monitor.FailureMonitorFactory;
import org.apache.hadoop.hbase.snapshot.monitor.RunningSnapshotFailureMonitor;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotErrorCheckable;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotTimer;

/**
 * Tracking the global status of a snapshot over its lifetime. Each snapshot is
 * associated with a SnapshotSentinel. It is created at the start of snapshot
 * and is destroyed when the snapshot reaches one of the end status:
 * <code>GlobalSnapshotStatus.ALL_RS_FINISHED<code> or
 * <code>GlobalSnapshotStatus.ABORTED<code>.
 */
@InterfaceAudience.Private
public abstract class SnapshotSentinel extends SnapshotErrorCheckable {

  /** By default, check to see if the snapshot is complete every 50ms */
  public static final int DEFAULT_MAX_WAIT_FREQUENCY = 500;

  /** Conf key for frequency to check for snapshot error while waiting for completion */
  public static final String MASTER_MAX_WAKE_FREQUENCY = "hbase.snapshot.master.wakeFrequency";

  private SnapshotTimer timer;
  private SnapshotDescriptor snapshot;
  private RunningSnapshotFailureMonitor errorMonitor;
  protected final long wakeFrequency;

  /**
   * Create the sentinel to montior the given snapshot on the table
   * @param hsd snapshot to take
   * @param master parent services to configure the sentinel
   * @throws IOException
   */
  SnapshotSentinel(final SnapshotDescriptor hsd, final MasterServices master) throws IOException {
    super(hsd);
    this.wakeFrequency = master.getConfiguration().getInt(MASTER_MAX_WAKE_FREQUENCY,
      DEFAULT_MAX_WAIT_FREQUENCY);
    this.snapshot = hsd;
    this.errorMonitor = new FailureMonitorFactory(master.getConfiguration(), true)
        .getRunningSnapshotFailureMonitor(hsd, this);
    // make sure we listen for snapshot failures from the running snapshot
    this.addSnapshotFailureListener(errorMonitor);
  }

  /**
   * Kickoff the snapshot.
   * @throws IOException if the snapshot fails
   */
  public final void run() throws IOException {
    try {
      // start the timer
      this.timer = this.errorMonitor.getTimerErrorMonitor();
      this.timer.start();
      // run the snapshot
      this.snapshot();
      // tell the timer to stop
      this.timer.complete();
    } catch (SnapshotCreationException e) {
      this.errorMonitor.snapshotFailure(this.snapshot,
        "Failed to run snapshot from sentinel:" + e.getMessage());
      throw e;
    } catch (IOException e) {
      this.errorMonitor.snapshotFailure(this.snapshot,
        "Failed to run snapshot from sentinel:" + e.getMessage());
      throw e;
    }
  }

  /**
   * Take the snapshot
   * @throws IOException if the snapshot could not be completed
   */
  protected abstract void snapshot() throws IOException;

  @Override
  public String toString() {
    return this.snapshot.toString();
  }
}
