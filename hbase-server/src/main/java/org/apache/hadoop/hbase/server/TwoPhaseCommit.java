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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.server.error.ErrorMonitor;

/**
 * General two-phase commit orchestrator.
 * <p>
 * Designed to be be run asynchronously such you can have multiple parts of a two-phase commit all
 * working together. For instance, a single server is writing multiple files (all part of the same
 * operation) and these files need to be written in parallel, but they shouldn't be moved to the
 * final directory if they all aren't written. In this case you could create multiple
 * {@link TwoPhaseCommit}, one for each file, and elect one of the processes as the 'leader'. Each
 * file is written on its own {@link Thread} in the prepare stage, counting down on the master's
 * commit latch. When the leader finishes writing its file two possible situations can happen:
 * <ol>
 * <li>All files have been written (commit latch count == 0)</li>
 * <li>Not all files have been written (commit latch count > 0)</li>
 * </ol>
 * In the first case, the leader is then free to perform a commit step and move the files to the
 * commit location. In the latter case, the leader blocks, on its own thread, waiting for all the
 * writers to finish, and only when they finish does the leader commit the files.
 * @param <E> Type of exception that any stage of the commit can throw
 */
public abstract class TwoPhaseCommit<E extends Exception> implements Callable<Void> {

  private static final Log LOG = LogFactory.getLog(TwoPhaseCommit.class);

  private final CountDownLatch preparedLatch;
  private final CountDownLatch commitLatch;
  private final CountDownLatch finishedLatch;
  private final long wakeFrequency;

  private ErrorMonitor<E> errorHandler;

  /**
   * Constructor that has prepare, commit and finish latch counts of 1.
   * @param handler notified if there is an error in the commit
   * @param wakeFrequency frequency to wake to check if there is an error between elements
   */
  public TwoPhaseCommit(ErrorMonitor<E> handler, long wakeFrequency) {
    this(handler, wakeFrequency, 1, 1, 1);
  }

  public TwoPhaseCommit(ErrorMonitor<E> handler, long wakeFrequency, int numPrepare,
      int numCommit, int numFinish) {
    this.errorHandler = handler;
    this.wakeFrequency = wakeFrequency;
    this.preparedLatch = new CountDownLatch(numPrepare);
    this.commitLatch = new CountDownLatch(numCommit);
    this.finishedLatch = new CountDownLatch(numFinish);
  }

  public CountDownLatch getPreparedLatch() {
    return this.preparedLatch;
  }

  public CountDownLatch getCommittedLatch() {
    return this.commitLatch;
  }

  public CountDownLatch getFinishedLatch() {
    return this.finishedLatch;
  }

  /**
   * Prepare a snapshot on the region.
   * @throws SnapshotCreationException
   */
  protected abstract void prepare() throws E;

  /**
   * Commit the snapshot - indicator from master that the snapshot can complete locally.
   * @throws SnapshotCreationException
   */
  protected abstract void commit() throws E;

  /**
   * Cleanup any state that may have changed from {@link #prepare()} to {@link #commit()}. This is
   * guaranteed to run under failure situations after {@link #prepare()} has been called.
   */
  protected abstract void cleanup();

  @Override
  public Void call() throws Exception {
    try {
      // get ready for snapshot
      LOG.debug("Starting 'prepare' stage of two phase commit");
      prepare();
      errorHandler.failOnError();

      // notify that we are prepared to snapshot
      LOG.debug("Prepare stage completed, counting down prepare.");
      this.preparedLatch.countDown();

      // wait for the indicator that we should commit
      LOG.debug("Counted down prepare, waiting on commit latch to complete.");
      waitForLatch(commitLatch, "COMMIT");

      // get the files for commit
      errorHandler.failOnError();
      LOG.debug("Commit latch released, running commit step.");
      commit();

      errorHandler.failOnError();
      LOG.debug("Commit completed, counting down finsh latch.");
      this.finishedLatch.countDown();
    }  finally {
      // reset the state of the region, as necessary
      this.cleanup();
    }
    return null;
  }

  /**
   * Wait for latch to count to zero, ignoring any spurious wake-ups, but waking periodically to
   * check for errors
   * @param latch latch to wait on
   * @throws E if the task was failed while waiting
   */
  protected void waitForLatch(CountDownLatch latch, String latchType) throws E {
    ThreadUtils.waitForLatch(latch, errorHandler, wakeFrequency, latchType);
  }
}
