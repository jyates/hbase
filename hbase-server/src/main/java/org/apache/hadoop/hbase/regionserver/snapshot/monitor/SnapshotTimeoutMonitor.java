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

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * 
 */
public class SnapshotTimeoutMonitor<T> extends ErrorMonitor<T> {
  private final long maxTime;
  private final long startTime;
  private volatile boolean complete;

  public SnapshotTimeoutMonitor(Class<T> caller, long now, long maxTime) {
    super(caller);
    this.maxTime = maxTime;
    this.startTime = now;
  }

  @Override
  public boolean checkForError() {
    // true< if the running time is within the valid limits
    return complete ? false : timeLapse() >= maxTime;
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
}
