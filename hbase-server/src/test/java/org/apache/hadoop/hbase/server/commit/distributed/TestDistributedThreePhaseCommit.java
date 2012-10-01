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

import static org.apache.hadoop.hbase.server.ServerThreads.waitForLatch;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.never;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.RemoteFailureException;
import org.apache.hadoop.hbase.server.ServerThreads;
import org.apache.hadoop.hbase.server.commit.ThreePhaseCommit;
import org.apache.hadoop.hbase.server.commit.distributed.cohort.CohortMemberTaskBuilder;
import org.apache.hadoop.hbase.server.commit.distributed.cohort.DistributedThreePhaseCommitCohortMember;
import org.apache.hadoop.hbase.server.commit.distributed.coordinator.CoordinatorTask;
import org.apache.hadoop.hbase.server.commit.distributed.coordinator.CoordinatorTaskBuilder;
import org.apache.hadoop.hbase.server.commit.distributed.coordinator.DistributedThreePhaseCommitCoordinator;
import org.apache.hadoop.hbase.server.commit.distributed.zookeeper.ZKTwoPhaseCommitCohortMemberController;
import org.apache.hadoop.hbase.server.commit.distributed.zookeeper.ZKTwoPhaseCommitCoordinatorController;
import org.apache.hadoop.hbase.server.errorhandling.exception.OperationAttemptTimeoutException;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionSnare;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.internal.matchers.ArrayEquals;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;

import com.google.common.collect.Lists;

/**
 * Cluster-wide testing of a distributed three-phase commit using a 'real' zookeeper cluster
 */
@Category(MediumTests.class)
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestDistributedThreePhaseCommit {

  private static final Log LOG = LogFactory.getLog(TestDistributedThreePhaseCommit.class);
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final String COORDINATOR_NODE_NAME = "coordinator";
  private static final long KEEP_ALIVE = 100;
  private static final int POOL_SIZE = 1;
  private static final long TIMEOUT = 10000000;
  private static final long WAKE_FREQUENCY = 500;
  private static final Object[] TIMEOUT_INFO = new Object[] { "timeout!" };
  private static final String opName = "op";
  private static final byte[] data = new byte[] { 1, 2 };
  private static final VerificationMode once = Mockito.times(1);

  @BeforeClass
  public static void setupTest() throws Exception {
    UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    UTIL.shutdownMiniZKCluster();
  }

  private static ZooKeeperWatcher newZooKeeperWatcher() throws IOException {
    return new ZooKeeperWatcher(UTIL.getConfiguration(), "testing utility", new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        throw new RuntimeException(
            "Unexpected abort in distributed three phase commit test:" + why, e);
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    });
  }

  @Test
  public void testEmptyCohort() throws Exception {
    runSimpleCommit();
  }

  @Test
  public void testSingleMemberCohort() throws Exception {
    runSimpleCommit("one");
  }

  @Test
  public void testMultipleMemberCohort() throws Exception {
    runSimpleCommit("one", "two", "three", "four" );
  }

  private void runSimpleCommit(String... cohortNames) throws Exception {
    // make sure we just have an empty list
    if (cohortNames == null) cohortNames = new String[0];

    // setup the constants
    ZooKeeperWatcher coordinatorWatcher = newZooKeeperWatcher();
    String opDescription = "simple coordination test - " + cohortNames.length + " cohort members";

    List<String> expected = Arrays.asList(cohortNames);

    // start running the controller
    ZKTwoPhaseCommitCoordinatorController coordinatorController = new ZKTwoPhaseCommitCoordinatorController(
        coordinatorWatcher, opDescription, COORDINATOR_NODE_NAME);
    CoordinatorTaskBuilder coordinatorTaskBuilder = Mockito.spy(new CoordinatorTaskBuilder(
        WAKE_FREQUENCY, TIMEOUT, TIMEOUT_INFO));
    
    DistributedThreePhaseCommitCoordinator coordinator = new DistributedThreePhaseCommitCoordinator(
        COORDINATOR_NODE_NAME, KEEP_ALIVE, POOL_SIZE, WAKE_FREQUENCY, coordinatorController,
        coordinatorTaskBuilder);
    coordinatorController.start(coordinator);

    CohortMemberTaskBuilder cohortMemberBuilder = Mockito.mock(CohortMemberTaskBuilder.class);
    List<Pair<DistributedThreePhaseCommitCohortMember, ZKTwoPhaseCommitCohortMemberController>> cohort = new ArrayList<Pair<DistributedThreePhaseCommitCohortMember, ZKTwoPhaseCommitCohortMemberController>>(
        cohortNames.length);
    // start a cohort member for each cohort member
    for (String member : cohortNames) {
      ZooKeeperWatcher watcher = newZooKeeperWatcher();
      ZKTwoPhaseCommitCohortMemberController controller = new ZKTwoPhaseCommitCohortMemberController(
          watcher, opDescription, member);
      DistributedThreePhaseCommitCohortMember manager = new DistributedThreePhaseCommitCohortMember(
          WAKE_FREQUENCY, KEEP_ALIVE, POOL_SIZE, controller, cohortMemberBuilder, member);
      cohort
          .add(new Pair<DistributedThreePhaseCommitCohortMember, ZKTwoPhaseCommitCohortMemberController>(
              manager, controller));
      controller.start(manager);
    }

    // setup mock cohort member tasks
    final List<ThreePhaseCommit> cohortTasks = new ArrayList<ThreePhaseCommit>();
    for (int i = 0; i < cohort.size(); i++) {
      DistributedThreePhaseCommitErrorDispatcher cohortMonitor = new DistributedThreePhaseCommitErrorDispatcher();
      ThreePhaseCommit commit = spiedCommitTask(cohortMonitor);
      cohortTasks.add(commit);
    }

    final int[] i = new int[] { 0 };
    Mockito.when(
      cohortMemberBuilder.buildNewOperation(Mockito.eq(opName),
        (byte[]) Mockito.argThat(new ArrayEquals(data)))).thenAnswer(
      new Answer<ThreePhaseCommit>() {
        @Override
        public ThreePhaseCommit answer(InvocationOnMock invocation) throws Throwable {
          // sync around the counter to ensure we can't get multiple calls to the same task
          synchronized (i) {
            // get the next task member
            int index = i[0];
            LOG.debug("Tasks size:" + cohortTasks.size() + ", getting:" + index);
            ThreePhaseCommit commit = cohortTasks.get(index);
            index++;
            i[0] = index;
            return commit;
          }
        }
      });
    
    // setup spying on the coordinator
    CoordinatorTask coordinatorTask = Mockito.spy(coordinatorTaskBuilder.buildOperation(
      coordinator, opName, data, expected));
    Mockito.when(coordinatorTaskBuilder.buildOperation(coordinator, opName, data, expected))
        .thenReturn(coordinatorTask);

    // start running the operation
    CoordinatorTask task = coordinator.kickOffCommit(opName, data, expected);
    assertEquals("Didn't mock coordinator task", coordinatorTask, task);

    // verify all things ran as expected
    waitAndVerifyTask(coordinatorTask, once, once, never(), once, false);
    verifyCohortSuccessful(expected, cohortMemberBuilder, cohortTasks, once, once, never(),
      once,
      false);

    // close all the things
    closeAll(coordinator, coordinatorController, cohort);
  }

  /**
   * @return a {@link ThreePhaseCommit} that is wrapped via {@link Mockito#spy(Object)}.
   */
  private ThreePhaseCommit spiedCommitTask(DistributedThreePhaseCommitErrorDispatcher cohortMonitor) {
    return Mockito
        .spy(new ThreePhaseCommit<DistributedThreePhaseCommitErrorDispatcher, DistributedCommitException>(
            cohortMonitor, cohortMonitor, WAKE_FREQUENCY, TIMEOUT) {
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
        });
  }

  /**
   * Test a distributed commit with multiple cohort members, where one of the cohort members has a
   * timeout exception during the prepare stage.
   * @throws Exception
   */
  @Test
  public void testMultiCohortWithMemberTimeoutDuringPrepare() throws Exception {
    String opDescription = "error injection coordination";
    String[] cohortMembers = new String[] { "one", "two", "three" };
    List<String> expected = Lists.newArrayList(cohortMembers);
    // error constants
    final int memberErrorIndex = 2;
    final CountDownLatch coordinatorReceivedErrorLatch = new CountDownLatch(1);

    // start running the coordinator and its controller
    ZooKeeperWatcher coordinatorWatcher = newZooKeeperWatcher();
    ZKTwoPhaseCommitCoordinatorController coordinatorController = new ZKTwoPhaseCommitCoordinatorController(
        coordinatorWatcher, opDescription, COORDINATOR_NODE_NAME);
    CoordinatorTaskBuilder coordinatorTaskBuilder = Mockito.spy(new CoordinatorTaskBuilder(
        WAKE_FREQUENCY, TIMEOUT, TIMEOUT_INFO));
    DistributedThreePhaseCommitCoordinator coordinator = new DistributedThreePhaseCommitCoordinator(
        COORDINATOR_NODE_NAME, KEEP_ALIVE, POOL_SIZE, WAKE_FREQUENCY, coordinatorController,
        coordinatorTaskBuilder);
    coordinatorController.start(coordinator);

    // start a cohort member for each cohort member
    CohortMemberTaskBuilder cohortMemberBuilder = Mockito.mock(CohortMemberTaskBuilder.class);
    List<Pair<DistributedThreePhaseCommitCohortMember, ZKTwoPhaseCommitCohortMemberController>> cohort = new ArrayList<Pair<DistributedThreePhaseCommitCohortMember, ZKTwoPhaseCommitCohortMemberController>>(
        expected.size());
    for (String member : expected) {
      ZooKeeperWatcher watcher = newZooKeeperWatcher();
      ZKTwoPhaseCommitCohortMemberController controller = new ZKTwoPhaseCommitCohortMemberController(
          watcher, opDescription, member);
      DistributedThreePhaseCommitCohortMember manager = new DistributedThreePhaseCommitCohortMember(
          WAKE_FREQUENCY, KEEP_ALIVE, POOL_SIZE, controller, cohortMemberBuilder, member);
      cohort
          .add(new Pair<DistributedThreePhaseCommitCohortMember, ZKTwoPhaseCommitCohortMemberController>(
              manager, controller));
      controller.start(manager);
    }

    // setup mock cohort member tasks
    final List<ThreePhaseCommit> cohortTasks = new ArrayList<ThreePhaseCommit>();
    final int[] elem = new int[1];
    for (int i = 0; i < cohort.size(); i++) {
      DistributedThreePhaseCommitErrorDispatcher cohortMonitor = new DistributedThreePhaseCommitErrorDispatcher();
      ThreePhaseCommit commit = spiedCommitTask(cohortMonitor);
      Mockito.doAnswer(new Answer() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          int index = elem[0];
          if (index == memberErrorIndex) {
            LOG.debug("Sending error to coordinator");
            ((ThreePhaseCommit<DistributedThreePhaseCommitErrorDispatcher, ?>) invocation.getMock())
                .getErrorListener().operationTimeout(new OperationAttemptTimeoutException(1, 2, 0));
            // don't complete the error phase until the coordinator has gotten the error
            // notification (which ensures that we never progress past prepare)
            ServerThreads.waitForLatchUninterruptibly(coordinatorReceivedErrorLatch,
              new ExceptionSnare(),
              WAKE_FREQUENCY, "coordinator received error");
          }
          elem[0] = ++index;
          return null;
        }
      }).when(commit).prepare();
      cohortTasks.add(commit);
    }

    // pass out a task per member
    final int[] i = new int[] { 0 };
    Mockito.when(
      cohortMemberBuilder.buildNewOperation(Mockito.eq(opName),
        (byte[]) Mockito.argThat(new ArrayEquals(data)))).thenAnswer(
      new Answer<ThreePhaseCommit>() {
        @Override
        public ThreePhaseCommit answer(InvocationOnMock invocation) throws Throwable {
          int index = i[0];
          ThreePhaseCommit commit = cohortTasks.get(index);
          index++;
          i[0] = index;
          return commit;
        }
      });

    // setup spying on the coordinator
    DistributedThreePhaseCommitErrorDispatcher coordinatorTaskErrorMonitor = Mockito
        .spy(new DistributedThreePhaseCommitErrorDispatcher());
    CoordinatorTask coordinatorTask = Mockito.spy(new CoordinatorTask(coordinator,
        coordinatorController, coordinatorTaskErrorMonitor, WAKE_FREQUENCY, TIMEOUT, TIMEOUT_INFO,
        opName, data, expected));
    Mockito.when(coordinatorTaskBuilder.buildOperation(coordinator, opName, data, expected))
        .thenReturn(coordinatorTask);
    // count down the error latch when we get the remote error
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        // pass on the error to the master
        invocation.callRealMethod();
        // then count down the got error latch
        coordinatorReceivedErrorLatch.countDown();
        return null;
      }
    }).when(coordinatorTaskErrorMonitor)
        .remoteCommitError(Mockito.any(RemoteFailureException.class));

    // ----------------------------
    // start running the operation
    // ----------------------------

    CoordinatorTask task = coordinator.kickOffCommit(opName, data, expected);
    assertEquals("Didn't mock coordinator task", coordinatorTask, task);

    // wait for the task to complete
    ServerThreads.waitForLatchUninterruptibly(task.getCompletedLatch(),
      new ExceptionSnare<Exception>(),
      WAKE_FREQUENCY,
      "coordinator task completed");


    // -------------
     // verification
     // -------------
    waitAndVerifyTask(coordinatorTask, once, never(), once, once, true);
    verifyCohortSuccessful(expected, cohortMemberBuilder, cohortTasks, once, never(), once,
      once,
      true);

    // close all the open things
    closeAll(coordinator, coordinatorController, cohort);
  }

  /**
   * Wait for the coordinator task to complete, and verify all the mocks
   * @param task to wait on
   * @throws Exception on unexpected failure
   */
  private void waitAndVerifyTask(ThreePhaseCommit op, VerificationMode prepare,
      VerificationMode commit, VerificationMode cleanup, VerificationMode finish, boolean opHasError)
      throws Exception {
    ServerThreads.waitForLatchUninterruptibly(op.getCompletedLatch(),
      new ExceptionSnare<Exception>(),
      WAKE_FREQUENCY, "task completed");
    // make sure that the task called all the expected phases
    Mockito.verify(op, prepare).prepare();
    Mockito.verify(op, commit).commit();
    Mockito.verify(op, cleanup).cleanup(Mockito.any(Exception.class));
    Mockito.verify(op, finish).finish();
    assertEquals("Operation error state was unexpected", opHasError, op.getErrorCheckable()
        .checkForError());
  }

  private void verifyCohortSuccessful(List<String> cohortNames,
      CohortMemberTaskBuilder cohortMemberBuilder, Iterable<ThreePhaseCommit> cohortTasks,
      VerificationMode prepare, VerificationMode commit, VerificationMode cleanup,
      VerificationMode finish, boolean opHasError) throws Exception {

    // make sure we build the correct number of cohort members
    Mockito.verify(cohortMemberBuilder, Mockito.times(cohortNames.size())).buildNewOperation(
      Mockito.eq(opName), (byte[]) Mockito.argThat(new ArrayEquals(data)));
    // verify that we ran each of the operations cleanly
    int j = 0;
    for (ThreePhaseCommit op : cohortTasks) {
      LOG.debug("Checking mock:" + (j++));
      waitAndVerifyTask(op, prepare, commit, cleanup, finish, opHasError);
    }
  }

  private void closeAll(
      DistributedThreePhaseCommitCoordinator coordinator,
      ZKTwoPhaseCommitCoordinatorController coordinatorController,
      List<Pair<DistributedThreePhaseCommitCohortMember, ZKTwoPhaseCommitCohortMemberController>> cohort)
      throws IOException {
    // make sure we close all the resources
    for (Pair<DistributedThreePhaseCommitCohortMember, ZKTwoPhaseCommitCohortMemberController> member : cohort) {
      member.getFirst().close();
      member.getSecond().close();
    }
    coordinator.close();
    coordinatorController.close();
  }

}
