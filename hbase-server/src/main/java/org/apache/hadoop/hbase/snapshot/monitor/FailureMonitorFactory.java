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
package org.apache.hadoop.hbase.snapshot.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.master.snapshot.monitor.MasterSnapshotFailureMonitor;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Factory that produces failure monitors for a given snapshot request
 */
public class FailureMonitorFactory {

  private final Configuration conf;
  private final boolean onMaster;

  public FailureMonitorFactory(Configuration conf, boolean runningOnMaster) {
    this.conf = conf;
    this.onMaster = runningOnMaster;
  }

  /**
   * Get a monitor for the given snapshot. Ensures external errors propagate down and internal
   * errors propagate out
   * @param snapshot to be monitored
   * @param externalError external listener for a snapshot error
   * @return a {@link SnapshotFailureListenable} to monitor the given snapshot for external and
   *         internal errors.
   */
  public RunningSnapshotFailureMonitor getRunningSnapshotFailureMonitor(
      SnapshotDescriptor snapshot, SnapshotFailureListener externalError) {
    RunningSnapshotFailureMonitor errorMonitor;
    // this could be more OO, but we are only ever going to run a snapshot on the RS or Master, so
    // having different classes for each (at the moment) is overkill.
    if (onMaster) {
      errorMonitor = new MasterSnapshotFailureMonitor(snapshot, externalError, conf);
    } else {
      errorMonitor = new GlobalSnapshotErrorMonitor(snapshot, externalError, conf);
    }

    return errorMonitor;
  }

  private class GlobalSnapshotErrorMonitor extends RunningSnapshotFailureMonitor {

    /**
     * @param snapshot snapshot that will be monitored
     * @param externalError error listener to propagate errors out of this
     *          snapshot
     * @param conf Configuration to read for timeout information
     */
    public GlobalSnapshotErrorMonitor(SnapshotDescriptor snapshot,
        SnapshotFailureListener externalError, Configuration conf) {
      super(snapshot, conf, externalError);
    }

    @Override
    public SnapshotTimer getTimerErrorMonitor() {
      // bind the snapshot timer to this monitor, so we propagate the error
      // internally and back out to ZK, if it occurs
      long maxTime = this.snapshot.getType().getMaxRegionTimeout(conf, SnapshotDescriptor.DEFAULT_REGION_SNAPSHOT_TIMEOUT);
      SnapshotTimer timer = new SnapshotTimer(snapshot, maxTime);
      timer.addSnapshotFailureListener(this);
      return timer;
    }
  }
}
