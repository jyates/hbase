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
package org.apache.hadoop.hbase.server.error;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Time a given process/operation and report a failure if the elapsed time exceeds the max allowed
 * time.
 */
public class ProcessTimer implements ErrorListener {

  private final long maxTime;
  private volatile boolean complete;
  private final Timer timer;
  private TimerTask timerTask;
  private long start;
  private ErrorListener dispatcher;

  public ProcessTimer(ErrorListener dispatcher, final long maxTime, final Object ...info) {
    this.dispatcher = dispatcher;
    this.maxTime = maxTime;
    timer = new Timer();
    timerTask = new TimerTask() {
      @Override
      public void run() {
        if (!ProcessTimer.this.complete) {
          long end = System.currentTimeMillis();
          ProcessTimer.this.receiveError("Timeout elapsed! Start:" + start
              + ", End:" + end + ", diff:" + (end - start) + ", max:" + maxTime, info);
        }
      }
    };
  }

  /**
   * For all time forward, do not throw an error because the process has completed.
   */
  public void complete() {
    this.complete = true;
    this.timer.cancel();
  }

  /**
   * Start a timer to fail a process if it takes longer than the expected time to complete.
   * Non-blocking call.
   */
  public void start() {
    timer.schedule(timerTask, maxTime);
    this.start = System.currentTimeMillis();
  }

  @Override
  public void receiveError(String message, Object... info) {
    // just pass through the error to the dispatcher
    this.dispatcher.receiveError(message, info);
  }
}