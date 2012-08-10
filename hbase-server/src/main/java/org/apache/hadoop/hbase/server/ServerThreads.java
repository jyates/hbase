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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;

/**
 * Thread utilities for server-side processing
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ServerThreads {
  private static final Log LOG = LogFactory.getLog(ServerThreads.class);

  /**
   * Wait for latch to count to zero, ignoring any spurious wake-ups, but waking periodically to
   * check for errors
   * @param latch latch to wait on
   * @param monitor monitor to check for errors while waiting
   * @param wakeFrequency frequency to wake up and check for errors (in
   *          {@link TimeUnit#MILLISECONDS})
   * @param latchDescription description of the latch, for logging
   * @throws E type of error the monitor can throw, if the task fails
   * @throws InterruptedException if we are interrupted while waiting on latch
   */
  public static <E extends Exception> void waitForLatch(CountDownLatch latch,
      ExceptionCheckable<E> monitor, long wakeFrequency, String latchDescription) throws E,
      InterruptedException {
    boolean released = false;
    while (!released) {
      if (monitor != null) monitor.failOnError();
      LOG.debug("Waiting for '" + latchDescription + "' latch. (sleep:" + wakeFrequency + " ms)");
      released = latch.await(wakeFrequency, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Wait for latch to count to zero, ignoring any spurious wake-ups, but waking periodically to
   * check for errors
   * @param latch latch to wait on
   * @param monitor monitor to check for errors while waiting
   * @param wakeFrequency frequency to wake up and check for errors (in
   *          {@link TimeUnit#MILLISECONDS})
   * @param latchDescription description of the latch, for logging
   * @throws E type of error the monitor can throw, if the task fails
   */
  public static <E extends Exception> void waitForLatchUninterruptibly(CountDownLatch latch,
      ExceptionCheckable<E> monitor, long wakeFrequency, String latchDescription) throws E {
    try {
      waitForLatch(latch, monitor, wakeFrequency, latchDescription);
    } catch (InterruptedException e) {
      LOG.debug("Wait for latch interrupted, done:" + (latch.getCount() == 0));
      // reset the interrupt status on the thread
      Thread.currentThread().interrupt();
    }
  }
}
