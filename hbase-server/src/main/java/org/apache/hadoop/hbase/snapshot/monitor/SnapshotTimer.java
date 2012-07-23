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
package org.apache.hadoop.hbase.snapshot.monitor;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Time a given snapshot and report a failure if the snapshot exceeds the max allowed time
 */
public class SnapshotTimer extends SnapshotErrorPropagator {

  private final long maxTime;
  private volatile boolean complete;
  private final Timer timer;
  private TimerTask timerTask;
  private long start;

  public SnapshotTimer(final SnapshotDescriptor snapshot, final long maxTime) {
    this.maxTime = maxTime;
    timer = new Timer();
    timerTask = new TimerTask() {
      @Override
      public void run() {
        if (!SnapshotTimer.this.complete) {
          long end = System.currentTimeMillis();
          SnapshotTimer.this.snapshotFailure(snapshot, "Snapshot timeout elapsed! Start:" + start
              + ", End:" + end + ", diff:" + (end - start) + ", max:" + maxTime);
        }
      }
    };
  }

  /**
   * For all time forward, do not throw an error
   */
  public void complete() {
    this.complete = true;
    this.timer.cancel();
  }

  /**
   * Start a timer to fail a snapshot if it takes longer than the expected time to complete
   */
  public void start() {
    timer.schedule(timerTask, maxTime);
    this.start = System.currentTimeMillis();
  }
}