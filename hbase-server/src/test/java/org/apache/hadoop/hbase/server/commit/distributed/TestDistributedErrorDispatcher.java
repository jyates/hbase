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

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.protobuf.generated.DistributedCommitProtos.CommitPhase;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.RemoteFailureException;
import org.apache.hadoop.hbase.server.errorhandling.exception.OperationAttemptTimeoutException;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.internal.matchers.ArrayEquals;

/**
 * Test that we dispatch errors as expected
 */
@Category(SmallTests.class)
public class TestDistributedErrorDispatcher {

  private static final DistributedThreePhaseCommitErrorListener mock = Mockito
      .mock(DistributedThreePhaseCommitErrorListener.class);

  @After
  public void resetMocks() {
    Mockito.reset(mock);
  }

  private static DistributedThreePhaseCommitErrorDispatcher getNewDispatcherWithMock() {
    DistributedThreePhaseCommitErrorDispatcher dispatcher = new DistributedThreePhaseCommitErrorDispatcher();
    dispatcher.addErrorListener(mock);
    return dispatcher;
  }

  @Test
  public void testTimeout() {
    DistributedThreePhaseCommitErrorDispatcher dispatcher = getNewDispatcherWithMock();
    OperationAttemptTimeoutException cause = new OperationAttemptTimeoutException(1, 2, 0);
    dispatcher.operationTimeout(cause);
    Mockito.verify(mock, Mockito.times(1)).operationTimeout(cause);
    Mockito.verifyNoMoreInteractions(mock);
  }

  @Test
  public void testControllerFailure() {
    DistributedThreePhaseCommitErrorDispatcher dispatcher = getNewDispatcherWithMock();
    IOException cause = new IOException("controller fail");
    dispatcher.controllerConnectionFailure("Test fail", cause);
    Mockito.verify(mock, Mockito.times(1)).controllerConnectionFailure("Test fail", cause);
    Mockito.verifyNoMoreInteractions(mock);
  }

  @Test
  public void testOperationException() {
    DistributedThreePhaseCommitErrorDispatcher dispatcher = getNewDispatcherWithMock();
    DistributedCommitException cause = new DistributedCommitException("Some op failure");
    dispatcher.localOperationException(CommitPhase.PREPARE, cause);
    Mockito.verify(mock, Mockito.times(1)).localOperationException(CommitPhase.PREPARE, cause);
    Mockito.verifyNoMoreInteractions(mock);
  }

  @Test
  public void testRemoteException() {
    DistributedThreePhaseCommitErrorDispatcher dispatcher = getNewDispatcherWithMock();
    RemoteFailureException remote = RemoteExceptionSerializer.buildRemoteException("someNode",
      new OperationAttemptTimeoutException(1, 2, 0));
    dispatcher.remoteCommitError(remote);
    Mockito.verify(mock, Mockito.times(1)).remoteCommitError(
      Mockito.argThat(new RemoteFailureMatcher(remote)));
    Mockito.verifyNoMoreInteractions(mock);
  }

  @Test
  public void testMultipleExceptions() {
    DistributedThreePhaseCommitErrorDispatcher dispatcher = getNewDispatcherWithMock();
    IOException controller = new IOException("controller fail");
    OperationAttemptTimeoutException timeout = new OperationAttemptTimeoutException(1, 2, 0);
    DistributedCommitException op = new DistributedCommitException("Some op failure");

    // pass all the test failures to the dispatcher
    dispatcher.controllerConnectionFailure("Test fail", controller);
    dispatcher.operationTimeout(timeout);
    dispatcher.localOperationException(CommitPhase.COMMIT, op);

    // we should only get the first notification
    Mockito.verify(mock, Mockito.times(1)).controllerConnectionFailure("Test fail", controller);
    Mockito.verifyNoMoreInteractions(mock);
  }

  /**
   * Mockito/Hamcrest matcher that checks to see if the remote failure is byte-equivalent to the an
   * expected type.
   */
  private static class RemoteFailureMatcher extends BaseMatcher<RemoteFailureException> {

    private final RemoteFailureException expected;
    private final ArrayEquals equals;

    public RemoteFailureMatcher(RemoteFailureException e) {
      this.expected = e;
      equals = new ArrayEquals(e.toByteArray());
    }

    @Override
    public boolean matches(Object arg) {
      if (arg instanceof RemoteFailureException) {
        return equals.matches(((RemoteFailureException) arg).toByteArray());
      }
      return false;
    }

    @Override
    public void describeTo(Description describe) {
      describe.appendText("not the expected remote failure:" + expected);
    }
  }
}