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
package org.apache.hadoop.hbase.master.snapshot.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.snapshot.monitor.RunningSnapshotFailureMonitor;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotFailureListener;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotTimer;

/**
 * Failure monitor for the single threaded, offline table snapshot on the master.
 * <p>
 * The master doesn't need all the complex listener infrastructure of the RS, so we use this class
 * instead, though it could be munged into using those classes instead.
 */
public class MasterSnapshotFailureMonitor extends RunningSnapshotFailureMonitor {

  /**
   * Create a failure monitor
   * @param snapshot snapshot to be monitored
   * @param externalErrorListener listener for errors to the snapshot from outside the main snapshot
   *          process
   * @param conf configuration to get snapshot information
   */
  public MasterSnapshotFailureMonitor(SnapshotDescriptor snapshot,
      SnapshotFailureListener externalErrorListener, Configuration conf) {
    super(snapshot, conf, externalErrorListener);
  }

  @Override
  public SnapshotTimer getTimerErrorMonitor() {
    long maxWait = snapshot.getType().getMaxMasterTimeout(this.getConf(), SnapshotDescriptor.DEFAULT_MAX_WAIT_TIME);
    SnapshotTimer timer = new SnapshotTimer(snapshot, maxWait);
    timer.addSnapshotFailureListener(this);
    return timer;
  }
}
