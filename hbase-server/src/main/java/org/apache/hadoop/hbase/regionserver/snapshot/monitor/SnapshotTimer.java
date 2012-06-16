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
package org.apache.hadoop.hbase.regionserver.snapshot.monitor;

import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Time a given snapshot
 */
public class SnapshotTimer extends SnapshotErrorPropagator implements Runnable,
    SnapshotFailureListenable {

  private final long maxTime;
  private final long startTime;
  private volatile boolean complete;
  private final long wakeFrequency;
  private final SnapshotDescriptor snapshot;

  private SnapshotFailureListener listener;

  public SnapshotTimer(SnapshotDescriptor snapshot, SnapshotFailureListener failable,
      long wakeFrequency, long now, long maxTime) {
    this.maxTime = maxTime;
    this.startTime = now;
    this.listener = failable;
    this.snapshot = snapshot;
    this.wakeFrequency = wakeFrequency;
  }

  private long timeLapse() {
    long current = EnvironmentEdgeManager.currentTimeMillis();
    return (current - startTime);
  }

  /**
   * For all time forward, do not throw an error
   */
  public void complete() {
    this.complete = true;

  }

  @Override
  public synchronized void listenForSnapshotFailure(SnapshotFailureListener failable) {
    this.listener = failable;
  }

  @Override
  public void run() {
    do {
      try {
        Thread.sleep(wakeFrequency);
      } catch (InterruptedException e) {
        // Ignore
      }
    } while (!complete && timeLapse() < maxTime);
    if (!this.complete) {
      if (this.listener != null) {
        super.snapshotFailure(snapshot, "Snapshot timeout elapsed!");
      }
    }
  }
}
