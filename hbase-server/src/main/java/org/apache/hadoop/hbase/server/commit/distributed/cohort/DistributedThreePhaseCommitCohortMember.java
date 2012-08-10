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
package org.apache.hadoop.hbase.server.commit.distributed.cohort;

import static org.apache.hadoop.hbase.server.ServerThreads.waitForLatch;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.DistributedCommitProtos.CommitPhase;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.RemoteFailureException;
import org.apache.hadoop.hbase.server.commit.ThreePhaseCommit;
import org.apache.hadoop.hbase.server.commit.TwoPhaseCommit;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedCommitException;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedThreePhaseCommitErrorListener;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedThreePhaseCommitManager;
import org.apache.hadoop.hbase.server.commit.distributed.RemoteExceptionSerializer;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;
import org.apache.hadoop.hbase.server.errorhandling.exception.OperationAttemptTimeoutException;

/**
 * Process to kick off and manage a running {@link ThreePhaseCommit} cohort member. This is the part
 * of the three-phase commit that actually does the work and reports back to the coordinator when it
 * completes each phase.
 * <p>
 * If there is a connection error ({@link #controllerConnectionFailure(String, IOException)}) all
 * currently running operations are failed since we no longer can reach any other members as the
 * controller is down.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DistributedThreePhaseCommitCohortMember
    extends
    DistributedThreePhaseCommitManager<DistributedCommitCohortMemberController, ThreePhaseCommit<DistributedThreePhaseCommitErrorListener, DistributedCommitException>, DistributedThreePhaseCommitErrorListener>
    implements CohortMemberTaskRunner, Closeable {
  private static final Log LOG = LogFactory.getLog(DistributedThreePhaseCommitCohortMember.class);

  // thread pool information
  private static final int MIN_OP_THREADS = 2;
  private static final int OP_THREAD_MULTIPLIER = 2;

  private final CohortMemberTaskBuilder builder;
  private final long wakeFrequency;
  private final RemoteExceptionSerializer serializer;

  /**
   * @param wakeFrequency frequency in ms to check for errors in the operation
   * @param keepAlive amount of time to keep alive idle worker threads
   * @param opThreads max number of threads to use for running operations. In reality, we will
   *          create 2X the number of threads since we need to also run a monitor thread for each
   *          running operation
   * @param controller controller used to send notifications to the operation coordinator
   * @param builder build new distributed three phase commit operations on demand
   * @param nodeName name of the node to use when notifying the controller that an operation has
   *          prepared, committed, or aborted on operation
   */
  public DistributedThreePhaseCommitCohortMember(long wakeFrequency, long keepAlive, int opThreads,
      DistributedCommitCohortMemberController controller,
      CohortMemberTaskBuilder builder, String nodeName) {
    super(controller, nodeName, keepAlive, getOpThreads(opThreads), "cohort-member-" + nodeName);
    this.builder = builder;
    this.wakeFrequency = wakeFrequency;
    this.serializer = new RemoteExceptionSerializer(nodeName);
  }

  /**
   * Exposed for testing!
   * @param controller controller used to send notifications to the operation coordinator
   * @param nodeName name of the node to use when notifying the controller that an operation has
   *          prepared, committed, or aborted on operation
   * @param pool thread pool to submit tasks
   * @param builder build new distributed three phase commit operations on demand
   * @param wakeFrequency frequency in ms to check for errors in the operation
   */
  public DistributedThreePhaseCommitCohortMember(
      DistributedCommitCohortMemberController controller, String nodeName, ThreadPoolExecutor pool,
      CohortMemberTaskBuilder builder, long wakeFrequency) {
    super(controller, nodeName, pool);
    this.builder = builder;
    this.wakeFrequency = wakeFrequency;
    this.serializer = new RemoteExceptionSerializer(nodeName);
  }

  /**
   * Update the number of threads to run based on hard-limits.
   * <p>
   * <ol>
   * <li>&lt 0 means max value</li>
   * <li>&lt {@value #MIN_OP_THREADS} is set to @{value #MIN_OP_THREADS} to ensure we can currently
   * run operations</li>
   * <li>any other value is multiplied by {@value #OP_THREAD_MULTIPLIER} to ensure we can also run
   * monitor threads for each operation</li>
   * <ol>
   * @param opThreads supplied number of threads
   * @return the updated number of threads for the task pool
   */
  private static int getOpThreads(int opThreads) {
    if (opThreads < 0) {
      LOG.debug("Using an unbounded thread pool for running operations.");
      return Integer.MAX_VALUE;
    } else if (opThreads < MIN_OP_THREADS) {
      LOG.debug("Recieved op threads = " + opThreads + " setting to min: " + MIN_OP_THREADS
          + " so we can run monitor thread on the main operation.");
      return MIN_OP_THREADS;
    }
    int threads = opThreads * OP_THREAD_MULTIPLIER;
    LOG.debug("Setting max threads to: " + threads + " (" + OP_THREAD_MULTIPLIER + "x " + opThreads
        + " running operations) to allow thread montioring");
    // double the op threads since we need to concurrently run a montior with the operation as well,
    // out of the same pool, one monitor for each operation
    return threads;
  }

  @Override
  public void runNewOperation(String opName, byte[] data) {
    // build a new operation
    ThreePhaseCommit<DistributedThreePhaseCommitErrorListener, DistributedCommitException> commit = null;
    try {
      commit = builder.buildNewOperation(opName, data);
    } catch (IllegalArgumentException e) {
      abortOperationAttempt(opName, e);
    } catch (IllegalStateException e) {
      abortOperationAttempt(opName, e);
    }
    if (commit == null) {
      LOG.info("Operation:" + opName + " doesn't have a local task, not anything running here.");
      return;
    }
    // create a monitor to watch for operation failures
    Monitor monitor = new Monitor(opName, commit, commit.getErrorCheckable());
    DistributedThreePhaseCommitErrorListener dispatcher = commit.getErrorListener();
    // make sure the listener watches for errors to running operation
    dispatcher.addErrorListener(monitor);
    LOG.debug("Submitting new operation:" + opName);
    if (!this.submitOperation(dispatcher, opName, commit, Executors.callable(monitor))) {
      LOG.error("Failed to start operation:" + opName);
    }
  }

  /**
   * Attempt to abort the operation globally because of a specific exception.
   * @param opName name of the operation to fail
   * @param cause reason the operation failed
   * @throws RuntimeException if the controller throws an {@link IOException}
   */
  private void abortOperationAttempt(String opName, Exception cause) {
    LOG.error("Failed to start op: " + opName + ", failing globally.");
    try {
      this.controller.abortOperation(opName, this.serializer.buildFromUnreadableRemoteException(cause));
    } catch (IOException e) {
      LOG.error("Failed to abort operation!");
      throw new RuntimeException(e);
    }
  }

  /**
   * Notification that operation coordinator has reached the committed phase
   * @param opName name of the operation that should start running the commit phase
   */
  public void commitInitiated(String opName) {
    new NotifyListener(opName) {
      @Override
      protected void notifyOperation(
          ThreePhaseCommit<DistributedThreePhaseCommitErrorListener, DistributedCommitException> operation) {
        operation.getAllowCommitLatch().countDown();
      }

    }.run();
  }

  /**
   * Monitor thread for a running operation. Its responsible for tracking the progress of the local
   * operation progress. In the case of all things working correctly, the {@link Monitor} just waits
   * on the task's progress and notifies a {@link DistributedCommitCohortMemberController} of cohort
   * member's progress.
   * <p>
   * However, the monitor also tracks the errors the task encounters. The {@link Monitor} is bound
   * as a generic {@link DistributedThreePhaseCommitErrorListener} to the operation's
   * {@link ErrorMonitorable}, allowing it to get updates when the operation encounters errors.
   * <p>
   * Local errors are serialized and then propagated to the
   * {@link DistributedCommitCohortMemberController} so the controller and the rest of the cohort
   * can see the error and kill themselves in a timely manner (rather than waiting for the task
   * timer).
   * <p>
   * Remote errors already reach the {@link ErrorMonitorable} - failing the running operation -
   * allowing the {@link Monitor} to disregard calls to
   * {@link #remoteCommitError(RemoteFailureException)}. A similar logic also holds for calls to
   * {@link #controllerConnectionFailure(String, IOException)} (further, it doesn't make sense to
   * update the controller because it is won't reach any other cohort members).
   */
  private class Monitor extends Thread implements DistributedThreePhaseCommitErrorListener {

    private String opName;
    private CountDownLatch prepared;
    private CountDownLatch commit;
    private ExceptionCheckable<DistributedCommitException> errorChecker;

    public Monitor(String opName, TwoPhaseCommit<?, DistributedCommitException> operation,
        ExceptionCheckable<DistributedCommitException> checkable) {
      this.opName = opName;
      this.prepared = operation.getPreparedLatch();
      this.commit = operation.getCommitFinishedLatch();
      this.errorChecker = checkable;
    }

    @Override
    public void run() {
      try {
        LOG.debug("Running operation '" + opName + "' monitor, waiting for prepared");
        // wait for the prepared latch
        waitForLatch(prepared, errorChecker, wakeFrequency,
          "monitor waiting on operation ("
            + opName + ") prepared ");
        LOG.debug("prepared latch finished, notifying controller");
        // then notify the controller that we are prepared
        controller.prepared(opName);

        // then wait for the committed latch
        LOG.debug("Notified controller that we are prepared, waiting on commit latch.");
        waitForLatch(commit, errorChecker, wakeFrequency, "monitor waiting on operation ("
            + opName + ") committed");

        // notify the controller that we committed the operation
        controller.commited(opName);
        LOG.debug("Notified controller that we committed");
        // we can ignore the exceptions found while waiting because they will be passed to us as an
        // error listener for the general operation. Still logging them because its good to see the
        // propagation
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting - pool is shutdown. Killing monitor and task.");
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        LOG.error("Task '" + opName + "' failed.", e);
      }
    }

    @Override
    public void localOperationException(CommitPhase phase, DistributedCommitException cause) {
      LOG.error("Got a local opeation error, notifying controller");
      abort(serializer.buildRemoteException(phase, cause));
    }

    @Override
    public void operationTimeout(OperationAttemptTimeoutException cause) {
      LOG.error("Got a local opeation timeout, notifying controller");
      abort(serializer.buildRemoteException(cause));
    }

    /**
     * Pass an abort notification onto the actual controller to abort the operation
     * @param exceptionSerializer general exception class
     */
    private void abort(RemoteFailureException failureMessage) {
      try {
        controller.abortOperation(opName, failureMessage);
      } catch (IOException e) {
        // this will fail all the running operations, since the connection is down
        DistributedThreePhaseCommitCohortMember.this.controllerConnectionFailure(
          "Failed to abort operation:" + opName, e);
      }
    }

    @Override
    public void controllerConnectionFailure(String message, IOException cause) {
      LOG.error("Can't reach controller, not propagting error", cause);
    }

    @Override
    public void remoteCommitError(RemoteFailureException remoteCause) {
      LOG.error("Remote commit failure, not propagating error:" + remoteCause);
    }

    @Override
    public void addErrorListener(DistributedThreePhaseCommitErrorListener listener) {
      throw new UnsupportedOperationException("Cohort member monitor can't add listeners.");
    }
  }
}