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
package org.apache.hadoop.hbase.server.commit;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.DistributedCommitProtos.CommitPhase;
import org.apache.hadoop.hbase.server.ServerThreads;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionSnare;

/**
 * General two-phase commit task.
 * <p>
 * The non-error flow of operation is: {@link #prepare()}, count down {@link #getPreparedLatch()},
 * wait for {@link #getAllowCommitLatch()} to reach zero, {@link #commit()}, count down
 * {@link #getCommitFinishedLatch()}, {@link #finish()}, and finally count down
 * {@link #getCompletedLatch()}. This allows outside watchers to synchronize on each step of the
 * task, if they so desire, without having to add any extra latches/locks in your own
 * implementation.
 * <p>
 * <b>Error Handling</b>
 * <p>
 * <tt>this</tt> also has an {@link ExceptionCheckable} so it can monitor for errors to the running
 * operation. Periodically while running, the error monitor will be checked and the operation failed
 * with the exception thrown via {@link ExceptionCheckable#failOnError()}.
 * <p>
 * {@link TwoPhaseCommit} checks for exceptions before the operations starts, and periodically while
 * waiting for latches to release, and then after each phase. Note that if an error is received we
 * follow the same semantics for finding it as an {@link ExceptionSnare}.
 * <p>
 * If an error is found, {@link #cleanup(Exception)} is called with the found exception and then we
 * call {@link #finish()}, count down {@link #getCompletedLatch()}, and then re-throw the received
 * exception.
 * <p>
 * Note that {@link #finish()} will always be called, even if {@link #prepare()} has not yet been
 * called.
 * <p>
 * <b>Usage</b>
 * <p>
 * There are multiple use cases for <tt>this</tt>, both in terms of distributed and local operation.
 * For instance, a single server may write multiple files (all part of the same operation) and these
 * files need to be written in parallel, but they shouldn't be moved to the final directory if they
 * all aren't successfully written. In this case you could create multiple {@link TwoPhaseCommit},
 * one for each file, and elect one of the processes as the 'leader'. Each file is written on its
 * own {@link Thread} in the prepare stage, counting down on the master's commit latch. When the
 * leader finishes writing its file two possible situations can happen:
 * <ol>
 * <li>All files have been written (commit latch count == 0)</li>
 * <li>Not all files have been written (commit latch count > 0)</li>
 * </ol>
 * In the first case, the leader is then free to perform a commit step and move the files to the
 * commit location. In the latter case, the leader blocks, on its own thread, waiting for all the
 * writers to finish, and only when they finish does the leader commit the files.
 * <p>
 * With respect to error handling, the task should be considered done when the
 * {@link #getCompletedLatch()} reaches zero. At this point the operation has been committed locally
 * and all further error state is a local concern. In a distributed situation, no guarantees are
 * made that the error will be passed on, unless documented otherwise.
 * @param <L> Type of error listener to listen for errors to the operation
 * @param <E> Type of exception that any stage of the commit can throw
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class TwoPhaseCommit<L extends TwoPhaseCommitErrorListener<E>, E extends Exception>
    implements Callable<Void> {

  private static final Log LOG = LogFactory.getLog(TwoPhaseCommit.class);

  /** latch counted down when the prepared phase completes */
  private final CountDownLatch preparedLatch;
  /** waited on before allowing the commit phase to proceed */
  private final CountDownLatch allowCommitLatch;
  /** counted down when the commit phase completes */
  private final CountDownLatch commitFinishLatch;
  /** counted down when the {@link #finish()} phase completes */
  private final CountDownLatch completedLatch;
  /** monitor to check for errors */
  private final ExceptionCheckable<E> errorMonitor;
  /** listener to listen for any errors to the operation */
  private final L errorListener;
  /** The current phase the operation is in */
  protected CommitPhase phase = CommitPhase.PRE_PREPARE;
  /** frequency to check for errors (ms) */
  protected final long wakeFrequency;

  /**
   * Constructor that has prepare, commit and finish latch counts of 1.
   * @param monitor notified if there is an error in the commit
   * @param errorListener listener to listen for errors to the running operation
   * @param wakeFrequency frequency to wake to check if there is an error via the monitor (in
   *          milliseconds).
   */
  public TwoPhaseCommit(ExceptionCheckable<E> monitor, L errorListener,
      long wakeFrequency) {
    this(monitor, errorListener, wakeFrequency, 1, 1, 1, 1);
  }

  /**
   * Create an operation with a specific size of the prepare, commit and finish latches. This is
   * useful for more complex internal processing that performs multiple pieces of functionality (or
   * coordinating with multiple external processes).
   * @param monitor error monitor used for:
   *          <ol>
   *          <li>check for an external error that would cause the commit to fail</li>
   *          <li>notified if there is an error while running</li>
   *          </ol>
   * @param errorListener listens for errors to the running operation
   * @param wakeFrequency frequency to check for operation errors in the handler while waiting on
   *          latches (in milliseconds)
   * @param numPrepare number of counts on the prepare latch (obtained via
   *          {@link #getPreparedLatch()}). This latch is counted down after the prepare phase
   *          completes.
   * @param numAllowCommit number of counts on the allow commit latch (latch obtained via
   *          {@link #getAllowCommitLatch()}). The {@link #commit()} phase will not get run (when
   *          used via {@link #call()}) until the 'allow commit' latch reaches zero.
   * @param numCommitted number of counts on the commit finished latch (latch obtained via
   *          {@link #getCommitFinishedLatch()}). This latch is counted down when the
   *          {@link #commit()} phase completes.
   * @param numCompleted number of counts on the completed latch (latch obtained via
   *          {@link #getCompletedLatch()}. This latch counts down after the {@link #finish()} phase
   *          completes.
   */
  public TwoPhaseCommit(ExceptionCheckable<E> monitor, L errorListener,
      long wakeFrequency, int numPrepare,
      int numAllowCommit, int numCommitted, int numCompleted) {
    this.errorMonitor = monitor;
    this.errorListener = errorListener;
    this.wakeFrequency = wakeFrequency;
    this.preparedLatch = new CountDownLatch(numPrepare);
    this.allowCommitLatch = new CountDownLatch(numAllowCommit);
    this.commitFinishLatch = new CountDownLatch(numCommitted);
    this.completedLatch = new CountDownLatch(numCompleted);
  }

  /**
   * When the {@link CommitPhase#PREPARE} phase completes, the latch returned here is counted down.
   * External/internal processes may also count-down this latch, so one must be aware of the count
   * prepare latch size passed in the constructor
   * @return latch counted down on complete of the prepare phase
   */
  public CountDownLatch getPreparedLatch() {
    return this.preparedLatch;
  }

  /**
   * The {@link CommitPhase#COMMIT} phase waits for this latch to reach zero. Some process (internal
   * or external) needs to count-down this latch to complete the operation.
   * @return latch blocking the commit phase
   */
  public CountDownLatch getAllowCommitLatch() {
    return this.allowCommitLatch;
  }

  /**
   * This latch is counted down after the {@link CommitPhase#COMMIT} phase completes.
   * @return the commit finished latch
   */
  public CountDownLatch getCommitFinishedLatch() {
    return this.commitFinishLatch;
  }

  /**
   * This latch is counted down after the {@link #finish()} phase completes
   * @return latch for completion of the overall operation.
   */
  public CountDownLatch getCompletedLatch() {
    return this.completedLatch;
  }

  public ExceptionCheckable<E> getErrorCheckable() {
    return this.errorMonitor;
  }

  /**
   * Prepare to commit the operation ({@link CommitPhase#PREPARE}). Isn't run if the monitor
   * presents an error when <tt>this</tt> is started.
   * @throws E on failure. Causes the {@link #cleanup(Exception)} phase to run with this exception
   */
  public abstract void prepare() throws E;

  /**
   * Run the {@link CommitPhase#COMMIT} operation. Only runs after the
   * {@link #getAllowCommitLatch()} reaches zero.
   * @throws E on failure. Causes the {@link #cleanup(Exception)} phase to run with this exception.
   */
  public abstract void commit() throws E;

  /**
   * Cleanup from a failure.
   * <p>
   * This method is called only if one of the earlier phases threw an error or an error was found in
   * the error monitor.
   * @param e exception thrown, either from an external source (found via
   *          {@link ExceptionSnare#failOnError()} or from the errors in internal operations,
   *          {@link #prepare()} {@link #commit()}.
   */
  public abstract void cleanup(Exception e);


  /**
   * Cleanup any state that may have changed from the start, {@link #prepare()} to {@link #commit()}
   * . This is guaranteed to run under failure situations after the operation has been started (but
   * not necessarily after {@link #prepare()} has been called).
   */
  public abstract void finish();

  @Override
  @SuppressWarnings("unchecked")
  public Void call() throws Exception {
    try {
      // start by checking for error first
      errorMonitor.failOnError();
      LOG.debug("Starting 'prepare' stage of two phase commit");
      phase = CommitPhase.PREPARE;
      prepare();

      // notify that we are prepared to snapshot
      LOG.debug("Prepare stage completed, counting down prepare.");
      this.getPreparedLatch().countDown();

      // wait for the indicator that we should commit
      LOG.debug("Waiting on 'commit allowed' latch to release.");
      phase = CommitPhase.PRE_COMMIT;

      // wait for the commit allowed latch to release
      waitForLatch(getAllowCommitLatch(), "commit allowed");
      errorMonitor.failOnError();

      LOG.debug("'Commit allowed' latch released, running commit step.");
      phase = CommitPhase.COMMIT;
      commit();

      // make sure we didn't get an error in commit
      phase = CommitPhase.POST_COMMIT;
      errorMonitor.failOnError();
      LOG.debug("Commit phase completed, counting down finsh latch.");
      this.getCommitFinishedLatch().countDown();
    } catch (Exception e) {
      LOG.error("Two phase commit failed!", e);
      errorListener.localOperationException(phase, (E) e);
      LOG.debug("Running cleanup phase.");
      this.cleanup(e);
      throw e;
    } finally {
      LOG.debug("Running finish phase.");
      this.finish();
      this.getCompletedLatch().countDown();
    }
    return null;
  }

  public L getErrorListener() {
    return errorListener;
  }

  /**
   * Wait for latch to count to zero, ignoring any spurious wake-ups, but waking periodically to
   * check for errors
   * @param latch latch to wait on
   * @param latchType String description of what the latch does
   * @throws E if the task was failed while waiting
   * @throws InterruptedException if we are interrupted while waiting for exception
   */
  public void waitForLatch(CountDownLatch latch, String latchType) throws E, InterruptedException {
    ServerThreads.waitForLatch(latch, errorMonitor, wakeFrequency, latchType);
  }
}