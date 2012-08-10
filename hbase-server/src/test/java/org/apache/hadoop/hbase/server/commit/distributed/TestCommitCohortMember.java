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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.protobuf.generated.DistributedCommitProtos.CommitPhase;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.RemoteFailureException;
import org.apache.hadoop.hbase.server.commit.ThreePhaseCommit;
import org.apache.hadoop.hbase.server.commit.distributed.cohort.CohortMemberTaskBuilder;
import org.apache.hadoop.hbase.server.commit.distributed.cohort.DistributedCommitCohortMemberController;
import org.apache.hadoop.hbase.server.commit.distributed.cohort.DistributedThreePhaseCommitCohortMember;
import org.apache.hadoop.hbase.server.errorhandling.exception.OperationAttemptTimeoutException;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test the general commit cohort member
 */
@Category(SmallTests.class)
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestCommitCohortMember {
  private static final long WAKE_FREQUENCY = 100;
  private static final long TIMEOUT = 100000;
  private static final long POOL_KEEP_ALIVE = 1;

  private final String op = "some op";
  private final byte[] data = new byte[0];
  private final DistributedThreePhaseCommitErrorDispatcher mockListener = Mockito
      .spy(new DistributedThreePhaseCommitErrorDispatcher());
  private final CohortMemberTaskBuilder mockBuilder = Mockito.mock(CohortMemberTaskBuilder.class);
  private final DistributedCommitCohortMemberController mockController = Mockito
      .mock(DistributedCommitCohortMemberController.class);

  private DistributedThreePhaseCommitCohortMember member;

  @After
  public void resetTest() {
    Mockito.reset(mockListener, mockBuilder, mockController);
    if (member != null) member.close();
  }

  /**
   * Build a cohort member using the class level mocks
   * @return member to use for tests
   */
  private DistributedThreePhaseCommitCohortMember buildCohortMember() {
    return new DistributedThreePhaseCommitCohortMember(WAKE_FREQUENCY, POOL_KEEP_ALIVE, 1,
        mockController, mockBuilder, "node");
  }

  /**
   * Add a 'start commit phase' response to the mock controller when it gets a prepared notification
   * @throws IOException on failure
   */
  private void addCommitAnswer() throws IOException {
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        member.commitInitiated(op);
        return null;
      }
    }).when(mockController).prepared(op);
  }

  /**
   * Make sure we properly fail when we reach too many requests
   * @throws Exception on unexpected failure
   */

  @Test
  public void testThreadPoolSize() throws Exception {
    member = buildCohortMember();

    // setup the operation
    ThreePhaseCommit commit = Mockito.mock(ThreePhaseCommit.class);
    CountDownLatch prepared = new CountDownLatch(1);
    CountDownLatch committed = new CountDownLatch(1);
    Mockito.when(commit.getPreparedLatch()).thenReturn(prepared);
    Mockito.when(commit.getCommitFinishedLatch()).thenReturn(committed);

    Mockito.when(mockBuilder.buildNewOperation(Mockito.anyString(), Mockito.eq(data))).thenReturn(
      commit);
    Mockito.when(commit.getErrorCheckable()).thenReturn(mockListener);
    Mockito.when(commit.getErrorListener()).thenReturn(mockListener);

    member.runNewOperation(op, data);
    member.runNewOperation("fail op", new byte[0]);
    // make sure we got an error on the error listener
    Mockito.verify(mockListener).localOperationException(Mockito.eq(CommitPhase.PRE_PREPARE),
      (DistributedCommitException) Mockito.any());
  }

  @Test(timeout = 500)
  public void testSimpleRun() throws Exception {
    member = buildCohortMember();

    ThreePhaseCommit commit = new EmptyThreePhaseCommit(mockListener);
    ThreePhaseCommit spy = Mockito.spy(commit);
    Mockito.when(mockBuilder.buildNewOperation(op, data)).thenReturn(spy);

    // when we get a prepare, then start the commit phase
    addCommitAnswer();

    // run the operation
    member.runNewOperation(op, data);
    // and wait for it to finish
    commit.getCompletedLatch().await();

    // make sure everything ran in order
    InOrder order = Mockito.inOrder(mockController, spy);
    // get the latches for the monitor
    order.verify(spy).getPreparedLatch();
    order.verify(spy).getCommitFinishedLatch();
    // that we prepared the operation
    order.verify(mockController).prepared(op);
    // once we prepare, we get the allow commit latch to count down
    order.verify(spy).getAllowCommitLatch();
    // then some time later we notify the controller that we committed
    order.verify(mockController).commited(op);
  }

  /**
   * Fail correctly on getting an external error while waiting for the prepared latch
   * @throws Exception on failure
   */
  @Test(timeout = 1000)
  public void testFailOnExternalErrorDuringPrepareWait() throws Exception {
    final DistributedThreePhaseCommitErrorDispatcher dispatcher = new DistributedThreePhaseCommitErrorDispatcher();
    ThreePhaseCommit commit = new EmptyThreePhaseCommit(dispatcher);
    ThreePhaseCommit spy = Mockito.spy(commit);

    CountDownLatch preparing = Mockito.mock(CountDownLatch.class);
    Mockito.when(spy.getPreparedLatch()).thenReturn(preparing);
    failOnExternalErrorDuringLatchWait(spy, preparing, dispatcher);

    // make sure everything ran in order
    InOrder order = Mockito.inOrder(mockController);
    order.verify(mockController, Mockito.never()).prepared(op);
    // make sure we don't pass on a remote exception back the controller
    order.verify(mockController, Mockito.never()).abortOperation(Mockito.matches(op),
      Mockito.any(RemoteFailureException.class));
    // make sure we don't attempt to commit when we find an exception
    order.verify(mockController, Mockito.never()).commited(op);
  }

  /**
   * Fail correctly on getting an external error while waiting for the commit allowed latch
   * @throws Exception on failure
   */
  @Test(timeout = 1000)
  public void testFailOnExternalErrorDuringCommitAllowedLatchWait() throws Exception {
    final DistributedThreePhaseCommitErrorDispatcher dispatcher = new DistributedThreePhaseCommitErrorDispatcher();
    ThreePhaseCommit commit = new EmptyThreePhaseCommit(dispatcher);
    ThreePhaseCommit spy = Mockito.spy(commit);
    CountDownLatch commitAllowed = Mockito.mock(CountDownLatch.class);
    Mockito.when(spy.getAllowCommitLatch()).thenReturn(commitAllowed);
    failOnExternalErrorDuringLatchWait(spy, commitAllowed, dispatcher);

    // make sure everything ran in order
    InOrder order = Mockito.inOrder(mockController);
    order.verify(mockController).prepared(op);
    // make sure we don't pass on a remote exception back the controller
    order.verify(mockController, Mockito.never()).abortOperation(Mockito.matches(op),
      Mockito.any(RemoteFailureException.class));
    // make sure we don't attempt to commit when we find an exception
    order.verify(mockController, Mockito.never()).commited(op);
  }

  /**
   * Fail correctly on getting an external error while waiting for the commit allowed latch. We may
   * actually get the commit latch to release while waiting, so we can't check that we didn't get a
   * commit notification on the controller. Everything is still correct, but just a little fuzzy.
   * @throws Exception on failure
   */
  @Test(timeout = 1000)
  public void testFailOnExternalErrorDuringCommitFinishedLatchWait() throws Exception {
    final DistributedThreePhaseCommitErrorDispatcher dispatcher = new DistributedThreePhaseCommitErrorDispatcher();
    ThreePhaseCommit commit = new EmptyThreePhaseCommit(dispatcher);
    ThreePhaseCommit spy = Mockito.spy(commit);
    CountDownLatch commitFinished = Mockito.mock(CountDownLatch.class);
    Mockito.when(spy.getCommitFinishedLatch()).thenReturn(commitFinished);
    failOnExternalErrorDuringLatchWait(spy, commitFinished, dispatcher);

    // make sure everything ran in order
    InOrder order = Mockito.inOrder(mockController);
    // make sure we prepare.
    order.verify(mockController).prepared(op);
    // make sure we don't pass on a remote exception back the controller
    order.verify(mockController, Mockito.never()).abortOperation(Mockito.matches(op),
      Mockito.any(RemoteFailureException.class));
  }

  private void failOnExternalErrorDuringLatchWait(ThreePhaseCommit spy, CountDownLatch errorLatch,
      final DistributedThreePhaseCommitErrorDispatcher dispatcher) throws Exception {
    Mockito.when(mockBuilder.buildNewOperation(op, data)).thenReturn(spy);
    member = buildCohortMember();
    // mock that another node timed out
    final RemoteFailureException remoteCause = RemoteExceptionSerializer.buildRemoteException(
      "some node", new OperationAttemptTimeoutException(1, 2, 0));
    Mockito.when(errorLatch.await(WAKE_FREQUENCY, TimeUnit.MILLISECONDS)).thenAnswer(
      new Answer<Boolean>() {
        @Override
        public Boolean answer(InvocationOnMock invocation) throws Throwable {
          // inject a remote error
          dispatcher.remoteCommitError(remoteCause);
          // sleep the wake frequency since that is what we promised
          Thread.sleep(WAKE_FREQUENCY);
          return false;
        }
      });

    // make sure we answer if we get a prepared response
    addCommitAnswer();

    // run the operation
    member.runNewOperation(op, data);
    // if the operation doesn't die properly, then this will timeout
    waitForCohortMemberToFinish(member);

    // check for an error in the dispatcher
    assertTrue("Error monitor didn't get the error notification", dispatcher.checkForError());
  }

  /**
   * Helper method to ensure correct waiting for a cohort member to finish an operation
   * @param member
   */
  private void waitForCohortMemberToFinish(DistributedThreePhaseCommitCohortMember member)
      throws Exception {
    // force a shutdown so we know the threads are trying to end
    member.getThreadPool().shutdown();
    // wait for all the threads to end
    member.getThreadPool().awaitTermination(TIMEOUT, TimeUnit.MINUTES);
  }

  /**
   * Fail correctly on getting an external error while waiting for the commit allowed latch. We may
   * actually get the commit latch to release while waiting, so we can't check that we didn't get a
   * commit notification on the controller. Everything is still correct, but just a little fuzzy.
   * @throws Exception on failure
   */
  @Test(timeout = 1000)
  public void testFailOnInternalErrorDuringPrepare() throws Exception {
    final DistributedThreePhaseCommitErrorDispatcher dispatcher = new DistributedThreePhaseCommitErrorDispatcher();
    ThreePhaseCommit commit = new EmptyThreePhaseCommit(dispatcher);
    ThreePhaseCommit spy = Mockito.spy(commit);
    Mockito.doThrow(new DistributedCommitException("prepare exception")).when(spy).prepare();

    // run the operation
    failOnInternalErrorDuringPhase(spy, dispatcher);

    // make sure everything ran in order
    InOrder order = Mockito.inOrder(mockController, spy);
    // make sure we prepare.
    order.verify(spy).prepare();
    order.verify(mockController, Mockito.never()).prepared(op);
    // make sure we pass a remote exception back the controller
    order.verify(mockController).abortOperation(Mockito.matches(op),
      Mockito.any(RemoteFailureException.class));
  }

  @Test(timeout = 1000)
  public void testFailOnInternalErrorDuringCommit() throws Exception {
    final DistributedThreePhaseCommitErrorDispatcher dispatcher = new DistributedThreePhaseCommitErrorDispatcher();
    ThreePhaseCommit commit = new EmptyThreePhaseCommit(dispatcher);
    ThreePhaseCommit spy = Mockito.spy(commit);
    Mockito.doThrow(new DistributedCommitException("commit exception")).when(spy).commit();

    // run the operation
    failOnInternalErrorDuringPhase(spy, dispatcher);

    // make sure everything ran in order
    InOrder order = Mockito.inOrder(mockController, spy);
    // make sure we prepare.
    order.verify(spy).prepare();
    order.verify(mockController).prepared(op);
    // make sure we never finish committing
    order.verify(spy).commit();
    order.verify(mockController, Mockito.never()).commited(op);
    // make sure we pass a remote exception back the controller
    order.verify(mockController).abortOperation(Mockito.matches(op),
      Mockito.any(RemoteFailureException.class));
  }

  private void failOnInternalErrorDuringPhase(ThreePhaseCommit spy,
      final DistributedThreePhaseCommitErrorDispatcher dispatcher) throws Exception {
    Mockito.when(mockBuilder.buildNewOperation(op, data)).thenReturn(spy);
    member = buildCohortMember();

    // make sure we answer if we get a prepared response
    addCommitAnswer();

    // run the operation
    member.runNewOperation(op, data);
    // if the operation doesn't die properly, then this will timeout
    waitForCohortMemberToFinish(member);

    // check for an error in the dispatcher
    assertTrue("Error monitor didn't get the error notification", dispatcher.checkForError());
  }

  /**
   * Fail correctly on getting an external error while waiting for the prepared latch
   * @throws Exception on failure
   */
  @Test(timeout = 1000)
  public void testPropagateConnectionErrorBackToManager() throws Exception {
    // setup the commit and the spy
    final DistributedThreePhaseCommitErrorDispatcher dispatcher = new DistributedThreePhaseCommitErrorDispatcher();
    ThreePhaseCommit commit = new EmptyThreePhaseCommit(dispatcher);
    ThreePhaseCommit spy = Mockito.spy(commit);

    // fail during the prepare phase
    Mockito.doThrow(new DistributedCommitException("prepare exception")).when(spy).prepare();
    // and throw a connection error when we try to tell the controller about it
    Mockito.doThrow(new IOException("Controller is down!")).when(mockController)
        .abortOperation(Mockito.eq(op), Mockito.any(RemoteFailureException.class));

    // setup the operation
    Mockito.when(mockBuilder.buildNewOperation(op, data)).thenReturn(spy);
    member = buildCohortMember();
    DistributedThreePhaseCommitCohortMember memberSpy = Mockito.spy(member);

    // run the operation
    memberSpy.runNewOperation(op, data);
    // if the operation doesn't die properly, then this will timeout
    waitForCohortMemberToFinish(memberSpy);

    // check for an error in the dispatcher
    assertTrue("Error monitor didn't get the error notification", dispatcher.checkForError());

    // make sure we got the connection failure error
    Mockito.verify(memberSpy).controllerConnectionFailure(Mockito.anyString(),
      Mockito.any(IOException.class));
    // make sure everything ran in order
    InOrder order = Mockito.inOrder(mockController, spy);
    // make sure we prepare.
    order.verify(spy).prepare();
    order.verify(mockController, Mockito.never()).prepared(op);
    // make sure we pass a remote exception back the controller
    order.verify(mockController).abortOperation(Mockito.matches(op),
      Mockito.any(RemoteFailureException.class));
  }

  /**
   * Test that the cohort member correctly doesn't attempt to start a task when the builder cannot
   * correctly build a new task for the requested operation
   * @throws Exception on failure
   */
  @Test
  public void testNoTaskToBeRunFromRequest() throws Exception {
    ThreadPoolExecutor pool = Mockito.mock(ThreadPoolExecutor.class);
    Mockito.when(mockBuilder.buildNewOperation(op, data)).thenReturn(null).thenThrow(new IllegalStateException("Wrong state!"), new IllegalArgumentException("can't understand the args"));
    member = new DistributedThreePhaseCommitCohortMember(mockController, "node",  pool, mockBuilder, WAKE_FREQUENCY);
    // builder returns null
    member.runNewOperation(op, data);
    // throws an illegal state exception
    member.runNewOperation(op, data);
    // throws an illegal argument exception
    member.runNewOperation(op, data);
    
    // no request should reach the pool
    Mockito.verifyZeroInteractions(pool);
    // get two abort requests
    Mockito.verify(mockController, Mockito.times(2)).abortOperation(Mockito.eq(op),
      Mockito.any(RemoteFailureException.class));
  }

  /**
   * Helper {@link ThreePhaseCommit} who's phase for each step is just empty
   */
  public class EmptyThreePhaseCommit extends
      ThreePhaseCommit<DistributedThreePhaseCommitErrorDispatcher, DistributedCommitException> {
    public EmptyThreePhaseCommit(DistributedThreePhaseCommitErrorDispatcher dispatcher) {
      super(dispatcher, dispatcher, WAKE_FREQUENCY, TIMEOUT);
    }

    @Override
    public void prepare() throws DistributedCommitException {
    }

    @Override
    public void commit() throws DistributedCommitException {
    }

    @Override
    public void cleanup(Exception e) {
    }

    @Override
    public void finish() {
    }
  }
}