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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedThreePhaseCommitErrorDispatcher;
import org.apache.hadoop.hbase.server.commit.distributed.controller.DistributedCommitCoordinatorController;
import org.apache.hadoop.hbase.server.commit.distributed.coordinator.CoordinatorTask;
import org.apache.hadoop.hbase.server.commit.distributed.coordinator.CoordinatorTaskBuilder;
import org.apache.hadoop.hbase.server.commit.distributed.coordinator.DistributedThreePhaseCommitCoordinator;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;

/**
 * Test commit coordinator operation.
 * <p>
 * This only works correctly when we do <i>class level parallelization</i> of tests. If we do method
 * level serialization this class will likely throw all kinds of errors.
 */
@Category(SmallTests.class)
public class TestCommitCoordinator {
  // general test constants
  private static final long WAKE_FREQUENCY = 1000;
  private static final long TIMEOUT = 100000;
  private static final Object[] TIMEOUT_INFO = new Object[] { "timeout!" };
  private static final long POOL_KEEP_ALIVE = 1;
  private static final String nodeName = "node";
  private static final String opName = "some op";
  private static final byte[] opData = new byte[0];
  private static final List<String> expected = Lists.newArrayList("remote1", "remote2");

  // setup the mocks
  private final CoordinatorTaskBuilder builder = Mockito.mock(CoordinatorTaskBuilder.class);
  private final DistributedCommitCoordinatorController controller = Mockito
      .mock(DistributedCommitCoordinatorController.class);
  private final CoordinatorTask task = Mockito.mock(CoordinatorTask.class);
  private final DistributedThreePhaseCommitErrorDispatcher monitor = Mockito
      .mock(DistributedThreePhaseCommitErrorDispatcher.class);

  // handle to the coordinator for each test
  private DistributedThreePhaseCommitCoordinator coordinator;

  @After
  public void resetTest() {
    // reset all the mocks used for the tests
    Mockito.reset(builder, controller, task, monitor);
    // close the open coordinator, if it was used
    if (coordinator != null) coordinator.close();
  }

  private DistributedThreePhaseCommitCoordinator buildNewCoordinator() {
    return Mockito.spy(new DistributedThreePhaseCommitCoordinator(nodeName, POOL_KEEP_ALIVE, 1,
        WAKE_FREQUENCY, controller, builder));
  }

  /**
   * Make sure we handle submitting more tasks than threads correctly
   * @throws Exception on failure
   */
  @Test
  public void testThreadPoolSize() throws Exception {
    DistributedThreePhaseCommitCoordinator coordinator = buildNewCoordinator();
    CoordinatorTask realTask = new CoordinatorTask(coordinator, controller, monitor,
        WAKE_FREQUENCY, TIMEOUT, TIMEOUT_INFO, opName, opData, expected);
    CoordinatorTask spy = Mockito.spy(realTask);
    Mockito.when(
      builder.buildOperation(Mockito.eq(coordinator), Mockito.anyString(), Mockito.eq(opData),
        Mockito.eq(expected))).thenReturn(spy, task);
    Mockito.when(task.getErrorListener()).thenReturn(monitor);
    coordinator.kickOffCommit(opName, opData, expected);
    assertNull("Coordinator successfully ran two tasks at once with a single thread pool.",
      coordinator.kickOffCommit("another op", opData, expected));
  }

  /**
   * Check handling a connection failure correctly if we get it during the prepare phase
   * @throws Exception on failure
   */
  @Test(timeout = 500)
  public void testUnreachableControllerDuringPrepare() throws Exception {
    coordinator = buildNewCoordinator();

    DistributedThreePhaseCommitErrorDispatcher monitor = new DistributedThreePhaseCommitErrorDispatcher();
    DistributedThreePhaseCommitErrorDispatcher spyMonitor = Mockito.spy(monitor);
    // setup the task
    List<String> expected = Arrays.asList("cohort");
    CoordinatorTask task = new CoordinatorTask(coordinator, controller, spyMonitor, WAKE_FREQUENCY,
        TIMEOUT, TIMEOUT_INFO, opName, opData, expected);
    final CoordinatorTask spy = Mockito.spy(task);

    Mockito.when(
      builder.buildOperation(Mockito.eq(coordinator), Mockito.eq(opName), Mockito.eq(opData),
        Mockito.anyListOf(String.class))).thenReturn(spy);

    // use the passed controller responses
    IOException cause = new IOException("Failed to reach controller during prepare");
    Mockito.doThrow(cause).when(controller)
        .prepareOperation(Mockito.eq(opName), Mockito.eq(opData), Mockito.anyListOf(String.class));

    // run the operation
    task = coordinator.kickOffCommit(opName, opData, expected);
    // and wait for it to finish
    task.getCompletedLatch().await();
    Mockito.verify(spyMonitor, Mockito.times(1)).controllerConnectionFailure(Mockito.anyString(),
      Mockito.eq(cause));
    Mockito.verify(coordinator, Mockito.times(1)).controllerConnectionFailure(Mockito.anyString(),
      Mockito.eq(cause));
    Mockito.verify(controller, Mockito.times(1)).prepareOperation(opName, opData, expected);
    Mockito.verify(controller, Mockito.never()).commitOperation(Mockito.anyString(),
      Mockito.anyListOf(String.class));
  }

  /**
   * Check handling a connection failure correctly if we get it during the commit phase
   * @throws Exception on failure
   */
  @Test(timeout = 500)
  public void testUnreachableControllerDuringCommit() throws Exception {
    coordinator = buildNewCoordinator();
    // spy on the error monitor
    DistributedThreePhaseCommitErrorDispatcher monitor = Mockito
        .spy(new DistributedThreePhaseCommitErrorDispatcher());

    // setup the task and spy on it
    List<String> expected = Arrays.asList("cohort");
    final CoordinatorTask spy = Mockito.spy(new CoordinatorTask(coordinator, controller, monitor,
        WAKE_FREQUENCY, TIMEOUT, TIMEOUT_INFO, opName, opData, expected));

    Mockito.when(
      builder.buildOperation(Mockito.eq(coordinator), Mockito.eq(opName), Mockito.eq(opData),
        Mockito.anyListOf(String.class))).thenReturn(spy);
    Mockito.when(spy.getErrorListener()).thenReturn(monitor);

    // use the passed controller responses
    IOException cause = new IOException("Failed to reach controller during prepare");
    Mockito.doAnswer(new PrepareOperationAnswer(opName, new String[] { "cohort" }))
        .when(controller)
        .prepareOperation(Mockito.eq(opName), Mockito.eq(opData), Mockito.anyListOf(String.class));
    Mockito.doThrow(cause).when(controller)
        .commitOperation(Mockito.eq(opName), Mockito.anyListOf(String.class));

    // run the operation
    CoordinatorTask task = coordinator.kickOffCommit(opName, opData, expected);
    // and wait for it to finish
    task.getCompletedLatch().await();
    Mockito.verify(monitor, Mockito.times(1)).controllerConnectionFailure(Mockito.anyString(),
      Mockito.eq(cause));
    Mockito.verify(coordinator, Mockito.times(1)).controllerConnectionFailure(Mockito.anyString(),
      Mockito.eq(cause));
    Mockito.verify(controller, Mockito.times(1)).prepareOperation(Mockito.eq(opName),
      Mockito.eq(opData), Mockito.anyListOf(String.class));
    Mockito.verify(controller, Mockito.times(1)).commitOperation(Mockito.anyString(),
      Mockito.anyListOf(String.class));
  }

  @Test(timeout = 1000)
  public void testNoCohort() throws Exception {
    runSimpleOrchestration();
  }

  @Test(timeout = 1000)
  public void testSingleCohortOrchestration() throws Exception {
    runSimpleOrchestration("one");
  }

  @Test(timeout = 1000)
  public void testMultipleCohortOrchestration() throws Exception {
    runSimpleOrchestration("one", "two", "three", "four");
  }

  public void runSimpleOrchestration(String... cohort) throws Exception {
    coordinator = buildNewCoordinator();
    CoordinatorTask task = new CoordinatorTask(coordinator, controller, monitor, WAKE_FREQUENCY,
        TIMEOUT, TIMEOUT_INFO, opName, opData, Arrays.asList(cohort));
    final CoordinatorTask spy = Mockito.spy(task);
    runCoordinatedOperation(spy, cohort);
  }

  /**
   * Test that if nodes join the commit barrier early we still correctly handle the progress
   * @throws Exception on failure
   */
  @Test(timeout = 1000)
  public void testEarlyJoiningCommitBarrier() throws Exception {
    final String[] cohort = new String[] { "one", "two", "three", "four" };
    coordinator = buildNewCoordinator();
    final DistributedThreePhaseCommitCoordinator ref = coordinator;
    CoordinatorTask task = new CoordinatorTask(coordinator, controller, monitor, WAKE_FREQUENCY,
        TIMEOUT, TIMEOUT_INFO, opName, opData, Arrays.asList(cohort));
    final CoordinatorTask spy = Mockito.spy(task);

    PrepareOperationAnswer prepare = new PrepareOperationAnswer(opName, cohort) {
      public void doWork() {
        // then do some fun where we commit before all nodes have prepared
        // "one" commits before anyone else is done
        ref.prepared(this.opName, this.cohort[0]);
        ref.committed(this.opName, this.cohort[0]);
        // but "two" takes a while
        ref.prepared(this.opName, this.cohort[1]);
        // "three"jumps ahead
        ref.prepared(this.opName, this.cohort[2]);
        ref.committed(this.opName, this.cohort[2]);
        // and "four" takes a while
        ref.prepared(this.opName, this.cohort[3]);
      }
    };

    CommitOperationAnswer commit = new CommitOperationAnswer(opName, cohort) {
      @Override
      public void doWork() {
        ref.committed(opName, this.cohort[1]);
        ref.committed(opName, this.cohort[3]);
      }
    };
    runCoordinatedOperation(spy, prepare, commit, cohort);
  }

  /**
   * Just run a simple operation with the standard name and data, with not special task for the mock
   * coordinator (it works just like a regular coordinator). For custom behavior see
   * {@link #runCoordinatedOperation(CoordinatorTask, PrepareOperationAnswer, CommitOperationAnswer, String[])}
   * .
   * @param spy Spy on a real {@link CoordinatorTask} to be returned by the mock
   *          {@link CoordinatorTaskBuilder}
   * @param cohort expected cohort members
   * @throws Exception on failure
   */
  public void runCoordinatedOperation(CoordinatorTask spy, String... cohort) throws Exception {
    runCoordinatedOperation(spy, new PrepareOperationAnswer(opName, cohort),
      new CommitOperationAnswer(opName, cohort), cohort);
  }

  public void runCoordinatedOperation(CoordinatorTask spy, PrepareOperationAnswer prepare,
      String... cohort) throws Exception {
    runCoordinatedOperation(spy, prepare, new CommitOperationAnswer(opName, cohort), cohort);
  }

  public void runCoordinatedOperation(CoordinatorTask spy, CommitOperationAnswer commit,
      String... cohort) throws Exception {
    runCoordinatedOperation(spy, new PrepareOperationAnswer(opName, cohort), commit, cohort);
  }

  public void runCoordinatedOperation(CoordinatorTask spy, PrepareOperationAnswer prepareOperation,
      CommitOperationAnswer commitOperation, String... cohort) throws Exception {
    List<String> expected = Arrays.asList(cohort);
    Mockito.when(
      builder.buildOperation(Mockito.eq(coordinator), Mockito.eq(opName), Mockito.eq(opData),
        Mockito.anyListOf(String.class))).thenReturn(spy);
    Mockito.when(task.getErrorListener()).thenReturn(monitor);

    // use the passed controller responses
    Mockito.doAnswer(prepareOperation).when(controller).prepareOperation(opName, opData, expected);
    Mockito.doAnswer(commitOperation).when(controller)
        .commitOperation(Mockito.eq(opName), Mockito.anyListOf(String.class));

    // run the operation
    CoordinatorTask task = coordinator.kickOffCommit(opName, opData, expected);
    // and wait for it to finish
    task.getCompletedLatch().await();

    // make sure we mocked correctly
    prepareOperation.ensureRan();
    // we never got an exception
    InOrder inorder = Mockito.inOrder(spy, controller);
    inorder.verify(spy).prepare();
    inorder.verify(controller).prepareOperation(opName, opData, expected);
    inorder.verify(spy).commit();
    inorder.verify(controller).commitOperation(Mockito.eq(opName), Mockito.anyListOf(String.class));
    Mockito.verify(spy, Mockito.never()).cleanup(Mockito.any(Exception.class));
  }

  private abstract class OperationAnswer implements Answer<Void> {
    private boolean ran = false;

    public void ensureRan() {
      assertTrue("Prepare mocking didn't actually run!", ran);
    }

    @Override
    public final Void answer(InvocationOnMock invocation) throws Throwable {
      this.ran = true;
      doWork();
      return null;
    }

    protected abstract void doWork() throws Throwable;
  }

  /**
   * Just tell the current coordinator that each of the nodes has prepared
   */
  private class PrepareOperationAnswer extends OperationAnswer {
    protected final String[] cohort;
    protected final String opName;

    public PrepareOperationAnswer(String opName, String... cohort) {
      this.cohort = cohort;
      this.opName = opName;
    }

    @Override
    public void doWork() {
      if (cohort == null) return;
      for (String member : cohort) {
        TestCommitCoordinator.this.coordinator.prepared(opName, member);
      }
    }
  }

  /**
   * Just tell the current coordinator that each of the nodes has committed
   */
  private class CommitOperationAnswer extends OperationAnswer {
    protected final String[] cohort;
    protected final String opName;

    public CommitOperationAnswer(String opName, String... cohort) {
      this.cohort = cohort;
      this.opName = opName;
    }

    @Override
    public void doWork() {
      if (cohort == null) return;
      for (String member : cohort) {
        TestCommitCoordinator.this.coordinator.committed(opName, member);
      }
    }
  }
}