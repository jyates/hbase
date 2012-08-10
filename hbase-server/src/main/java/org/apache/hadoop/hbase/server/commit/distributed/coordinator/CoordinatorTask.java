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
package org.apache.hadoop.hbase.server.commit.distributed.coordinator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.server.commit.ThreePhaseCommit;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedCommitException;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedThreePhaseCommitErrorDispatcher;
import org.apache.hadoop.hbase.server.commit.distributed.controller.DistributedCommitCoordinatorController;

import com.google.common.collect.Lists;

/**
 * A general coordinator task to run a distributed {@link ThreePhaseCommit} (internally it is also
 * is a {@link ThreePhaseCommit}).
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CoordinatorTask extends
    ThreePhaseCommit<DistributedThreePhaseCommitErrorDispatcher, DistributedCommitException> {

  private static final Log LOG = LogFactory.getLog(CoordinatorTask.class);

  /** lock to prevent nodes from preparing and then committing before we can track them */
  private Object joinBarrierLock = new Object();
  private final DistributedCommitCoordinatorController controller;
  private final List<String> prepareNodes;
  private final List<String> commitingNodes;
  private CountDownLatch allServersPreparedOperation;
  private CountDownLatch allServersCompletedOperation;
  private String opName;
  private byte[] data;

  private DistributedThreePhaseCommitCoordinator parent;

  /**
   * {@link ThreePhaseCommit} operation run by a {@link DistributedThreePhaseCommitCoordinator} for
   * a given operation
   * @param parent parent coordinator to call back to for general errors (e.g.
   *          {@link DistributedThreePhaseCommitCoordinator#controllerConnectionFailure(String, IOException)}
   *          ).
   * @param controller coordinator controller to update with progress
   * @param monitor error monitor to check for errors to the operation
   * @param wakeFreq frequency to check for errors while waiting
   * @param timeout amount of time to allow the operation to run
   * @param timeoutInfo information to pass along to the error monitor if there is a timeout
   * @param opName name of the operation to run
   * @param data information associated with the operation
   * @param expectedPrepared names of the expected cohort members
   */
  public CoordinatorTask(DistributedThreePhaseCommitCoordinator parent,
      DistributedCommitCoordinatorController controller,
      DistributedThreePhaseCommitErrorDispatcher monitor, long wakeFreq, long timeout,
      Object[] timeoutInfo, String opName, byte[] data, List<String> expectedPrepared) {
    super(1, expectedPrepared.size(), expectedPrepared.size(), 1, monitor, monitor, wakeFreq,
        timeout);
    this.parent = parent;
    this.controller = controller;
    this.prepareNodes = new ArrayList<String>(expectedPrepared);
    this.commitingNodes = new ArrayList<String>(prepareNodes.size());

    this.opName = opName;
    this.data = data;

    // block the commit phase until all the servers prepare
    this.allServersPreparedOperation = this.getAllowCommitLatch();
    // allow interested watchers to block until we are done with the commit phase
    this.allServersCompletedOperation = this.getCommitFinishedLatch();
  }

  @Override
  public void prepare() throws DistributedCommitException {
    // start the operation
    LOG.debug("running the prepare phase, kicking off prepare phase on cohort.");
    try {
      // prepare the operation, cloning the list to avoid a concurrent modification from the
      // controller setting the prepared nodes
      controller.prepareOperation(opName, data, Lists.newArrayList(this.prepareNodes));
    } catch (IOException e) {
      parent.controllerConnectionFailure("Can't reach controller.", e);
    } catch (IllegalArgumentException e) {
      throw new DistributedCommitException(e, new byte[0]);
    }
  }

  @Override
  public void commit() throws DistributedCommitException {
    try {
      // run the commit operation on the cohort
      controller.commitOperation(opName, Lists.newArrayList(this.commitingNodes));
    } catch (IOException e) {
      parent.controllerConnectionFailure("Can't reach controller.", e);
    }

    // then wait for the finish latch
    try {
      this.waitForLatch(this.allServersCompletedOperation, "cohort committed");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DistributedCommitException(e, new byte[0]);
    }
  }

  /**
   * Subclass hook for custom cleanup/rollback functionality - currently, a no-op.
   */
  @Override
  public void cleanup(Exception e) {
  }

  public void finish() {
    // remove ourselves from the running operations
    LOG.debug("Finishing coordinator operation - removing self from list of running operations");
    // then clear out the operation nodes
    LOG.debug("Reseting operation!");
    try {
      controller.resetOperation(opName);
    } catch (IOException e) {
      parent.controllerConnectionFailure("Failed to reset operation:" + opName, e);
    }
  }

  public void prepared(String node) {
    LOG.debug("node: '" + node + "' joining prepared barrier for operation '" + opName
        + "' on coordinator");
    if (this.prepareNodes.contains(node)) {
      synchronized (joinBarrierLock) {
        if (this.prepareNodes.remove(node)) {
          this.commitingNodes.add(node);
          this.allServersPreparedOperation.countDown();
        }
      }
      LOG.debug("Waiting on: " + allServersPreparedOperation
          + " remaining nodes to prepare operation");
    } else {
      LOG.debug("Node " + node + " joined operation, but we weren't waiting on it to join.");
    }
  }

  public void committed(String node) {
    boolean removed = false;
    synchronized (joinBarrierLock) {
      removed = this.commitingNodes.remove(node);
      this.allServersCompletedOperation.countDown();
    }
    if (removed) {
      LOG.debug("Node: '" + node + "' committed, counting down latch");
    } else {
      LOG.warn("Node: '" + node + "' committed operation, but we weren't waiting on it to commit.");
    }
  }
}