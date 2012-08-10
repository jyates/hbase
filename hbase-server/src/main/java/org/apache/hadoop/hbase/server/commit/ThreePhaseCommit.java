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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;
import org.apache.hadoop.hbase.server.errorhandling.OperationAttemptTimer;
import org.apache.hadoop.hbase.server.errorhandling.exception.OperationAttemptTimeoutException;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionSnare;

/**
 * A two-phase commit that enforces a time-limit on the operation. If the time limit expires before
 * the operation completes, the {@link ThreePhaseCommitErrorListenable} will receive a
 * {@link OperationAttemptTimeoutException} from the {@link OperationAttemptTimer}.
 * <p>
 * This is particularly useful for situations when running a distributed {@link TwoPhaseCommit} so
 * participants can avoid blocking for extreme amounts of time if one of the participants fails or
 * takes a really long time (e.g. GC pause).
 * @param <T> stored error listener to watch for errors from the operation
 * @param <E> type of exception the monitor will throw
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class ThreePhaseCommit<T extends ThreePhaseCommitErrorListenable<E>, E extends Exception>
    extends TwoPhaseCommit<T, E> {

  private static final Log LOG = LogFactory.getLog(ThreePhaseCommit.class);
  protected final OperationAttemptTimer timer;

  /**
   * Create a Three-Phase Commit operation
   * @param monitor error monitor to check for errors to the running operation
   * @param errorListener error listener to listen for errors while running the task
   * @param wakeFrequency frequency to check for errors while waiting for latches
   * @param timeout amount of time this operation is allowed to take before throwing an error and
   *          failing.
   */
  public ThreePhaseCommit(ExceptionCheckable<E> monitor, T errorListener, long wakeFrequency,
      long timeout) {
    super(monitor, errorListener, wakeFrequency);
    this.timer = setupTimer(errorListener, timeout);
  }

  /**
   * Create a Three-Phase Commit operation
   * @param numPrepare number of prepare latches (called when the prepare phase completes
   *          successfully)
   * @param numAllowCommit number of latches to wait on for the commit to proceed (blocks calls to
   *          the commit phase)
   * @param numCommitted number of commit completed latches should be established (called when the
   *          commit phase completes)
   * @param numCompleted number of completed latches to be established (called when everything has
   *          run)
   * @param monitor error monitor to check for errors to the running operation
   * @param errorListener listener to listen for any errors to the running operation
   * @param wakeFrequency frequency to check for errors while waiting for latches
   * @param timeout max amount of time to allow for the operation to run
   */
  public ThreePhaseCommit(int numPrepare, int numAllowCommit, int numCommitted, int numCompleted,
      ExceptionCheckable<E> monitor, T errorListener, long wakeFrequency, long timeout) {
    super(monitor, errorListener, wakeFrequency, numPrepare, numAllowCommit, numCommitted,
        numCompleted);
    this.timer = setupTimer(errorListener, timeout);
  }

  /**
   * Setup the process timeout. The timer is started whenever {@link #call()} or {@link #run()} is
   * called.
   * @param errorListener
   * @param timeoutInfo information to pass along when the timer expires
   * @param timeout max amount of time the operation is allowed to run before expiring.
   * @return a timer for the overall operation
   */
  private OperationAttemptTimer setupTimer(final ThreePhaseCommitErrorListenable<E> errorListener,
      long timeout) {
    // setup a simple monitor so we pass through the error to the specific listener
    ExceptionSnare<OperationAttemptTimeoutException> passThroughMonitor = new ExceptionSnare<OperationAttemptTimeoutException>() {
      @Override
      public void receiveError(String msg, OperationAttemptTimeoutException cause, Object... info) {
        errorListener.operationTimeout(cause);
      }
    };
    return new OperationAttemptTimer(passThroughMonitor, timeout);
  }

  @Override
  public Void call() throws Exception {
    LOG.debug("Starting three phase commit.");
    // start the timer
    this.timer.start();
    // run the operation
    super.call();
    // tell the timer we are done, if we get here successfully
    this.timer.complete();
    return null;
  }
}