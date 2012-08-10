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
package org.apache.hadoop.hbase.server.commit.distributed.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.server.commit.distributed.cohort.DistributedThreePhaseCommitCohortMember;
import org.apache.hadoop.hbase.server.commit.distributed.coordinator.DistributedThreePhaseCommitCoordinator;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionSnare;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
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
 * Test zookeeper based, three-phase commit controllers
 */
@Category(MediumTests.class)
public class TestZooKeeperDistributedThreePhaseCommitControllers {

  static final Log LOG = LogFactory.getLog(TestZooKeeperDistributedThreePhaseCommitControllers.class);
  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final String COHORT_NODE_NAME = "expected";
  private static final String CONTROLLER_NODE_NAME = "controller";
  private static final VerificationMode once = Mockito.times(1);

  private ZooKeeperWatcher watcher;

  @BeforeClass
  public static void setupTest() throws Exception {
    UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    UTIL.shutdownMiniZKCluster();
  }

  @Before
  public void setup() throws Exception {
    this.watcher = new ZooKeeperWatcher(UTIL.getConfiguration(), "testing utility",
        new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        throw new RuntimeException("Unexpected abort in HBaseTestingUtility:" + why, e);
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    });
  }

  @After
  public void cleanup() throws Exception {
    this.watcher.close();
  }

  @Test
  public void testLocallyManageZK() throws Exception {
    ZooKeeperWatcher watcherSpy = Mockito.spy(watcher);
    final ZKTwoPhaseCommitCohortMemberController controller = new ZKTwoPhaseCommitCohortMemberController(
        watcher, "testSimple", COHORT_NODE_NAME);
    controller.start();
    controller.close();
    // make sure we didn't call close on the watcher
    Mockito.verify(watcherSpy, times(0)).close();
  }

  /**
   * Smaller test to just test the actuation on the cohort member
   * @throws Exception on failure
   */
  @Test(timeout = 15000)
  public void testSimpleZKCohortMemberController() throws Exception {
    final String operationName = "instanceTest";
    final byte[] data = new byte[] { 1, 2, 3 };
    final CountDownLatch prepared = new CountDownLatch(1);
    final CountDownLatch committed = new CountDownLatch(1);
    final ExceptionSnare<Exception> monitor = spy(new ExceptionSnare<Exception>());
    final ZKTwoPhaseCommitCohortMemberController controller = new ZKTwoPhaseCommitCohortMemberController(
        watcher, "testSimple", COHORT_NODE_NAME);

    // mock out cohort member interactions
    final DistributedThreePhaseCommitCohortMember member = Mockito
        .mock(DistributedThreePhaseCommitCohortMember.class);
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        controller.prepared(operationName);
        prepared.countDown();
        return null;
      }
    }).when(member).runNewOperation(operationName, data);
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        controller.commited(operationName);
        committed.countDown();
        return null;
      }
    }).when(member).commitInitiated(operationName);

    // start running the listener
    controller.start(member);

    // set a prepare node from a 'coordinator'
    String prepare = ZKTwoPhaseCommitController.getPrepareBarrierNode(controller, operationName);
    ZKUtil.createSetData(watcher, prepare, data);
    // wait for the operation to be prepared
    prepared.await();

    // create the commit node so we update the operation to enter the commit phase
    String commit = ZKTwoPhaseCommitController.getCommitBarrierNode(controller, operationName);
    LOG.debug("Found prepared, posting commit node:" + commit);
    ZKUtil.createAndFailSilent(watcher, commit);
    LOG.debug("Commit node:" + commit + ", exists:" + ZKUtil.checkExists(watcher, commit));
    committed.await();

    verify(monitor, never()).receiveError(Mockito.anyString(), Mockito.any(Exception.class),
      Mockito.any());
    verify(member, never()).controllerConnectionFailure(Mockito.anyString(),
      Mockito.any(IOException.class));
    // cleanup after the test
    ZKUtil.deleteNodeRecursively(watcher, controller.baseZNode);
    assertEquals("Didn't delete prepare node", -1, ZKUtil.checkExists(watcher, prepare));
    assertEquals("Didn't delete commit node", -1, ZKUtil.checkExists(watcher, commit));
  }

  @Test(timeout = 15000)
  public void testZKCoordinatorControllerWithNoCohort() throws Exception {
    final String operationName = "no cohort controller test";
    final byte[] data = new byte[] { 1, 2, 3 };

    runMockCommitWithOrchestratedControllers(startCoordinatorFirst, operationName, data);
    runMockCommitWithOrchestratedControllers(startCohortFirst, operationName, data);
  }

  @Test(timeout = 15000)
  public void testZKCoordinatorControllerWithSingleMemberCohort() throws Exception {
    final String operationName = "single member controller test";
    final byte[] data = new byte[] { 1, 2, 3 };

    runMockCommitWithOrchestratedControllers(startCoordinatorFirst, operationName, data, "cohort");
    runMockCommitWithOrchestratedControllers(startCohortFirst, operationName, data, "cohort");
  }

  @Test(timeout = 15000)
  public void testZKCoordinatorControllerMultipleCohort() throws Exception {
    final String operationName = "multi member controller test";
    final byte[] data = new byte[] { 1, 2, 3 };

    runMockCommitWithOrchestratedControllers(startCoordinatorFirst, operationName, data, "cohort",
      "cohort2", "cohort3");
    runMockCommitWithOrchestratedControllers(startCohortFirst, operationName, data, "cohort",
      "cohort2", "cohort3");
  }

  private void runMockCommitWithOrchestratedControllers(StartControllers controllers,
      String operationName, byte[] data, String... cohort) throws Exception {
    List<String> expected = Lists.newArrayList(cohort);

    CountDownLatch prepared = new CountDownLatch(expected.size());
    CountDownLatch committed = new CountDownLatch(expected.size());
    // mock out coordinator so we can keep track of zk progress
    DistributedThreePhaseCommitCoordinator coordinator = setupMockCoordinator(operationName,
      prepared, committed);

    DistributedThreePhaseCommitCohortMember member = Mockito
        .mock(DistributedThreePhaseCommitCohortMember.class);

    Pair<ZKTwoPhaseCommitCoordinatorController, List<ZKTwoPhaseCommitCohortMemberController>> pair = controllers
        .start(watcher, operationName, coordinator, CONTROLLER_NODE_NAME, member, expected);
    ZKTwoPhaseCommitCoordinatorController controller = pair.getFirst();
    List<ZKTwoPhaseCommitCohortMemberController> cohortControllers = pair.getSecond();
    // start the operation
    controller.prepareOperation(operationName, data, expected);

    // post the prepare node for each expected node
    for (ZKTwoPhaseCommitCohortMemberController cc : cohortControllers) {
      cc.prepared(operationName);
    }

    // wait for all the notifications to reach the coordinator
    prepared.await();
    // make sure we got the all the nodes and no more
    Mockito.verify(coordinator, times(expected.size())).prepared(Mockito.eq(operationName),
      Mockito.anyString());

    // kick off the commit phase
    controller.commitOperation(operationName, expected);

    // post the committed node for each expected node
    for (ZKTwoPhaseCommitCohortMemberController cc : cohortControllers) {
      cc.commited(operationName);
    }

    // wait for all commit notifications to reach the coordiantor
    committed.await();
    // make sure we got the all the nodes and no more
    Mockito.verify(coordinator, times(expected.size())).committed(Mockito.eq(operationName),
      Mockito.anyString());

    controller.resetOperation(operationName);

    // verify all behavior
    verifyZooKeeperClean(operationName, watcher, controller);
    verifyCohort(member, cohortControllers.size(), operationName, data);
    verifyCoordinator(operationName, coordinator, expected);
  }

  @Test
  public void testCoordinatorControllerHandlesEarlyPrepareNodes() throws Exception {
    runEarlyPrepareNodes(startCoordinatorFirst, "testEarlyPreparenodes", new byte[] { 1, 2, 3 },
      "cohort1", "cohort2");
    runEarlyPrepareNodes(startCohortFirst, "testEarlyPreparenodes", new byte[] { 1, 2, 3 },
      "cohort1", "cohort2");
  }

  public void runEarlyPrepareNodes(StartControllers controllers, String operationName, byte[] data,
      String... cohort) throws Exception {
    List<String> expected = Lists.newArrayList(cohort);

    final CountDownLatch prepared = new CountDownLatch(expected.size());
    final CountDownLatch committed = new CountDownLatch(expected.size());
    // mock out coordinator so we can keep track of zk progress
    DistributedThreePhaseCommitCoordinator coordinator = setupMockCoordinator(operationName,
      prepared, committed);

    DistributedThreePhaseCommitCohortMember member = Mockito
        .mock(DistributedThreePhaseCommitCohortMember.class);

    Pair<ZKTwoPhaseCommitCoordinatorController, List<ZKTwoPhaseCommitCohortMemberController>> pair = controllers
        .start(watcher, operationName, coordinator, CONTROLLER_NODE_NAME, member, expected);
    ZKTwoPhaseCommitCoordinatorController controller = pair.getFirst();
    List<ZKTwoPhaseCommitCohortMemberController> cohortControllers = pair.getSecond();

    // post 1/2 the prepare nodes early
    for (int i = 0; i < cohortControllers.size() / 2; i++) {
      cohortControllers.get(i).prepared(operationName);
    }

    // start the operation
    controller.prepareOperation(operationName, data, expected);

    // post the prepare node for each expected node
    for (ZKTwoPhaseCommitCohortMemberController cc : cohortControllers) {
      cc.prepared(operationName);
    }

    // wait for all the notifications to reach the coordinator
    prepared.await();
    // make sure we got the all the nodes and no more
    Mockito.verify(coordinator, times(expected.size())).prepared(Mockito.eq(operationName),
      Mockito.anyString());

    // kick off the commit phase
    controller.commitOperation(operationName, expected);

    // post the committed node for each expected node
    for (ZKTwoPhaseCommitCohortMemberController cc : cohortControllers) {
      cc.commited(operationName);
    }

    // wait for all commit notifications to reach the coordiantor
    committed.await();
    // make sure we got the all the nodes and no more
    Mockito.verify(coordinator, times(expected.size())).committed(Mockito.eq(operationName),
      Mockito.anyString());

    controller.resetOperation(operationName);

    // verify all behavior
    verifyZooKeeperClean(operationName, watcher, controller);
    verifyCohort(member, cohortControllers.size(), operationName, data);
    verifyCoordinator(operationName, coordinator, expected);
  }

  /**
   * @return a mock {@link DistributedThreePhaseCommitCoordinator} that just counts down the
   *         prepared and committed latch for called to the respective method
   */
  private DistributedThreePhaseCommitCoordinator setupMockCoordinator(String operationName,
      final CountDownLatch prepared, final CountDownLatch committed) {
    DistributedThreePhaseCommitCoordinator coordinator = Mockito
        .mock(DistributedThreePhaseCommitCoordinator.class);
    Mockito.mock(DistributedThreePhaseCommitCoordinator.class);
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        prepared.countDown();
        return null;
      }
    }).when(coordinator).prepared(Mockito.eq(operationName), Mockito.anyString());
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        committed.countDown();
        return null;
      }
    }).when(coordinator).committed(Mockito.eq(operationName), Mockito.anyString());
    return coordinator;
  }

  /**
   * Verify that the prepare, commit and abort nodes for the operation are removed from zookeeper
   */
  private void verifyZooKeeperClean(String operationName, ZooKeeperWatcher watcher,
      ZKTwoPhaseCommitController<?> controller) throws Exception {
    String prepare = ZKTwoPhaseCommitController.getPrepareBarrierNode(controller, operationName);
    String commit = ZKTwoPhaseCommitController.getCommitBarrierNode(controller, operationName);
    String abort = ZKTwoPhaseCommitController.getAbortNode(controller, operationName);
    assertEquals("Didn't delete prepare node", -1, ZKUtil.checkExists(watcher, prepare));
    assertEquals("Didn't delete commit node", -1, ZKUtil.checkExists(watcher, commit));
    assertEquals("Didn't delete abort node", -1, ZKUtil.checkExists(watcher, abort));
  }

  /**
   * Verify the cohort controller got called once per expected node to start the operation
   */
  private void verifyCohort(DistributedThreePhaseCommitCohortMember member, int cohortSize,
      String operationName, byte[] data) {
    verify(member, Mockito.times(cohortSize)).runNewOperation(Mockito.eq(operationName),
      (byte[]) Mockito.argThat(new ArrayEquals(data)));
  }

  /**
   * Verify that the coordinator only got called once for each expected node
   */
  private void verifyCoordinator(String operationName,
      DistributedThreePhaseCommitCoordinator coordinator, List<String> expected) {
    // verify that we got all the expected nodes
    for (String node : expected) {
      verify(coordinator, once).prepared(operationName, node);
      verify(coordinator, once).committed(operationName, node);
    }
  }

  /**
   * Specify how the controllers that should be started (not spy/mockable) for the test.
   */
  private abstract class StartControllers {
    public abstract Pair<ZKTwoPhaseCommitCoordinatorController, List<ZKTwoPhaseCommitCohortMemberController>> start(
        ZooKeeperWatcher watcher, String operationName,
        DistributedThreePhaseCommitCoordinator coordinator, String controllerName,
        DistributedThreePhaseCommitCohortMember member, List<String> cohortNames) throws Exception;
  }

  private final StartControllers startCoordinatorFirst = new StartControllers() {

    @Override
    public Pair<ZKTwoPhaseCommitCoordinatorController, List<ZKTwoPhaseCommitCohortMemberController>> start(
        ZooKeeperWatcher watcher, String operationName,
        DistributedThreePhaseCommitCoordinator coordinator, String controllerName,
        DistributedThreePhaseCommitCohortMember member, List<String> expected) throws Exception {
      // start the controller
      ZKTwoPhaseCommitCoordinatorController controller = new ZKTwoPhaseCommitCoordinatorController(
          watcher, operationName, CONTROLLER_NODE_NAME);
      controller.start(coordinator);

      // make a cohort controller for each expected node

      List<ZKTwoPhaseCommitCohortMemberController> cohortControllers = new ArrayList<ZKTwoPhaseCommitCohortMemberController>();
      for (String nodeName : expected) {
        ZKTwoPhaseCommitCohortMemberController cc = new ZKTwoPhaseCommitCohortMemberController(
            watcher, operationName, nodeName);
        cc.start(member);
        cohortControllers.add(cc);
      }
      return new Pair<ZKTwoPhaseCommitCoordinatorController, List<ZKTwoPhaseCommitCohortMemberController>>(
          controller, cohortControllers);
    }
  };

  /**
   * Check for the possible race condition where a cohort member starts after the controller and
   * therefore could miss a new operation
   */
  private final StartControllers startCohortFirst = new StartControllers() {

    @Override
    public Pair<ZKTwoPhaseCommitCoordinatorController, List<ZKTwoPhaseCommitCohortMemberController>> start(
        ZooKeeperWatcher watcher, String operationName,
        DistributedThreePhaseCommitCoordinator coordinator, String controllerName,
        DistributedThreePhaseCommitCohortMember member, List<String> expected) throws Exception {

      // make a cohort controller for each expected node
      List<ZKTwoPhaseCommitCohortMemberController> cohortControllers = new ArrayList<ZKTwoPhaseCommitCohortMemberController>();
      for (String nodeName : expected) {
        ZKTwoPhaseCommitCohortMemberController cc = new ZKTwoPhaseCommitCohortMemberController(
            watcher, operationName, nodeName);
        cc.start(member);
        cohortControllers.add(cc);
      }

      // start the controller
      ZKTwoPhaseCommitCoordinatorController controller = new ZKTwoPhaseCommitCoordinatorController(
          watcher, operationName, CONTROLLER_NODE_NAME);
      controller.start(coordinator);

      return new Pair<ZKTwoPhaseCommitCoordinatorController, List<ZKTwoPhaseCommitCohortMemberController>>(
          controller, cohortControllers);
    }
  };
}