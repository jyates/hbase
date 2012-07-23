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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
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
public abstract class RunningSnapshotFailureMonitor extends SnapshotErrorCheckable implements
    Configurable {

  private Configuration conf;

  /**
   * @param snapshot
   * @param conf
   * @param listeners
   */
  public RunningSnapshotFailureMonitor(SnapshotDescriptor snapshot, Configuration conf,
      SnapshotFailureListener... listeners) {
    super(snapshot, listeners);
    this.conf = conf;
  }


  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }


  /**
   * Get a snapshot timer bound to this monitor
   * @return a {@link SnapshotTimer} to ensure the snapshot doesn't take too
   *         long
   */
  public abstract SnapshotTimer getTimerErrorMonitor();
}
