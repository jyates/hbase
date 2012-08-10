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
package org.apache.hadoop.hbase.server.commit.distributed;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.DistributedCommitProtos.CommitPhase;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.RemoteFailureException;
import org.apache.hadoop.hbase.server.commit.ThreePhaseCommit;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionVisitor;
import org.apache.hadoop.hbase.server.errorhandling.exception.OperationAttemptTimeoutException;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionDispatcher;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionSnare;
import org.apache.hadoop.hbase.server.errorhandling.impl.delegate.DelegatingExceptionDispatcher;

/**
 * Generic error listener for a distributed {@link ThreePhaseCommit}. Handles watching for errors
 * generally (and throwing the first found error on request) as well as propagating the first found
 * error to any listeners.
 * <p>
 * This can be used as the {@link ExceptionSnare} for the actual {@link ThreePhaseCommit} as it will
 * accept errors from the task itself as well as notice errors from the controller and remote
 * operation members (e.g. cohort members or the coordinator).
 * <p>
 * The raw {@link #receiveError(String, DistributedCommitException, Object...)} should <b>NOT</b> be
 * used externally - they are relied upon internally for message passing. Instead, you should use
 * methods specified in the listener interfaces.
 * @see DistributedThreePhaseCommitErrorListener
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DistributedThreePhaseCommitErrorDispatcher
    extends
    DelegatingExceptionDispatcher<ExceptionDispatcher<DistributedThreePhaseCommitErrorListener, DistributedCommitException>, DistributedThreePhaseCommitErrorListener, DistributedCommitException>
    implements DistributedThreePhaseCommitErrorListener {

  /**
   * Create a custom dispatcher that just uses
   * @param visitor visitor to use when notifying any listening
   *          {@link DistributedThreePhaseCommitErrorListener}
   */
  public DistributedThreePhaseCommitErrorDispatcher(
      ExceptionVisitor<DistributedThreePhaseCommitErrorListener> visitor) {
    super(
        new ExceptionDispatcher<DistributedThreePhaseCommitErrorListener, DistributedCommitException>(
            visitor));
  }

  /**
   * Create an error dispatcher for a distributed {@link ThreePhaseCommit}.
   */
  public DistributedThreePhaseCommitErrorDispatcher() {
    this(new DistributedCommitErrorListenerVisitor());
  }

  @Override
  public void operationTimeout(OperationAttemptTimeoutException cause) {
    this.receiveError("Got a timeout!", DistributedThreePhaseCommitErrorDispatcher.wrap(cause));
  }

  @Override
  public void localOperationException(CommitPhase phase, DistributedCommitException cause) {
    this.receiveError("Got an general operation failure!",
      DistributedThreePhaseCommitErrorDispatcher.wrap(cause), phase);
  }

  @Override
  public void controllerConnectionFailure(String message, IOException cause) {
    this.receiveError(message, DistributedThreePhaseCommitErrorDispatcher.wrap(cause));
  }

  @Override
  public void remoteCommitError(RemoteFailureException remoteCause) {
    RemoteExceptionSerializer.logRemoteCause(remoteCause);
    this.receiveError("Operation failed due to remote cause", wrap(remoteCause));
  }

  /**
   * Wrap a {@link RemoteFailureException} so it can be passed through error handlers. Unwrap via
   * {@link #unwrapRemoteException(DistributedCommitException)}
   * @param e remote cause to wrap
   * @return a {@link DistributedCommitException} to pass around
   */
  public static DistributedCommitException wrap(RemoteFailureException e) {
    return new DistributedCommitException(
        new DistributedThreePhaseCommitErrorDispatcher.RemoteException(e));
  }

  /**
   * Unwrap an exception that was wrapped via {@link #wrap}
   * @param e exception to examine
   * @return the wrapped exceptioon
   */
  public static Exception unwrap(DistributedCommitException e) {
    return (Exception) e.getCause();
  }

  static boolean isRemoteException(DistributedCommitException e) {
    return e.getCause() instanceof DistributedThreePhaseCommitErrorDispatcher.RemoteException;
  }

  /**
   * Unwrap a {@link RemoteFailureException} from the exception, if it it contains a
   * {@link RemoteFailureException} (as wrapped via
   * {@link DistributedThreePhaseCommitErrorDispatcher#wrap(RemoteFailureException)}.
   * @param e exception to examine
   * @return <tt>null</tt> if a {@link RemoteFailureException} wasn't wrapped, the wrapped value
   *         otherwise
   */
  public static RemoteFailureException unwrapRemoteException(DistributedCommitException e) {
    // make sure its a remote exception
    if (!DistributedThreePhaseCommitErrorDispatcher.isRemoteException(e)) return null;
    return ((DistributedThreePhaseCommitErrorDispatcher.RemoteException) e.getCause()).remoteCause;
  }

  /**
   * Helper method to wrap any exception in <tt>this</tt>
   * @param e exception to wrap
   * @return a {@link DistributedCommitException}
   */
  public static DistributedCommitException wrap(Exception e) {
    return new DistributedCommitException(e);
  }

  private static class DistributedCommitErrorListenerVisitor implements
      ExceptionVisitor<DistributedThreePhaseCommitErrorListener> {

    @Override
    public void visit(DistributedThreePhaseCommitErrorListener listener, String message,
        Exception e, Object... info) {
      // we need to wrap all the exceptions from the internal notification scheme, so we unwrap it
      // here and get the real exception
      DistributedCommitException expected = ((DistributedCommitException) e);
      RemoteFailureException cause = DistributedThreePhaseCommitErrorDispatcher
          .unwrapRemoteException(expected);
      // check for the remote failure case
      if (cause != null) {
        listener.remoteCommitError(cause);
        return;
      }
      Exception local = DistributedThreePhaseCommitErrorDispatcher.unwrap(expected);
      if (local instanceof OperationAttemptTimeoutException) {
        listener.operationTimeout((OperationAttemptTimeoutException) local);
      } else if (local instanceof IOException) {
        listener.controllerConnectionFailure(message, (IOException) local);
      } else {
        listener.localOperationException((CommitPhase) info[0], (DistributedCommitException) local);
      }
    }
  }

  /**
   * Helper method to demarcate a remote exception as actually being remote (rather than unwinding
   * the internal exception and expecting an operation exception to never have the same type).
   */
  @SuppressWarnings("serial")
  static class RemoteException extends Exception {
    RemoteFailureException remoteCause;

    public RemoteException(RemoteFailureException cause) {
      this.remoteCause = cause;
    }
  }
}