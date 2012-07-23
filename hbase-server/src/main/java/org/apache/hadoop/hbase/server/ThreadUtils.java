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
package org.apache.hadoop.hbase.server;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.server.error.ErrorMonitor;

/**
 * Utility class for dealing with multiple-process computation, synchronization and error handling.
 */
public class ThreadUtils {

  private static final Log LOG = LogFactory.getLog(ThreadUtils.class);

  private ThreadUtils() {
    // hidden ctor since its a utility class
  }

  /**
   * Wait for latch to count to zero, ignoring any spurious wake-ups, but waking periodically to
   * check for errors
   * @param latch latch to wait on
   * @param monitor monitor to check for errors while waiting
   * @param wakeFrequency frequency to make up and check for
   * @param latchType name of the latch, for logging
   * @throws E type of error the monitor can throw, if the task fails
   */
  public static <E extends Exception> void waitForLatch(CountDownLatch latch,
      ErrorMonitor<E> monitor, long wakeFrequency, String latchType) throws E {
    do {
      monitor.failOnError();
      try {
        LOG.debug("Waiting for snapshot " + latchType + " latch.");
        latch.await(wakeFrequency, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.debug("Wait for latch interrupted, done:" + (latch.getCount() == 0));
        // reset the interrupt status on the thread
        Thread.currentThread().interrupt();
      }
    } while (latch.getCount() > 0);
  }
}
