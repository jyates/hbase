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

import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedThreePhaseCommitErrorDispatcher;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedThreePhaseCommitManager;
import org.apache.hadoop.hbase.server.commit.distributed.controller.DistributedCommitCoordinatorController;

/**
 * General coordinator for a set of three-phase-commit operations (tasks).
 * <p>
 * This coordinator is used to launch multiple tasks of the same type (e.g. a snapshot) with
 * differences per instance specified via {@link #kickOffCommit(String, byte[], List)}.
 * <p>
 * NOTE: Tasks are run concurrently. To ensure a serial ordering of tasks, synchronize off the
 * completion of the task returned from {@link #kickOffCommit(String, byte[], List)}.
 * <p>
 * By default, a generic {@link CoordinatorTask} will be used to run the requested task, but a
 * custom task can be specified by a {@link CoordinatorTaskBuilder}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DistributedThreePhaseCommitCoordinator
    extends
    DistributedThreePhaseCommitManager<DistributedCommitCoordinatorController, CoordinatorTask, DistributedThreePhaseCommitErrorDispatcher> {

  private final DistributedCommitCoordinatorController controller;
  private CoordinatorTaskBuilder builder;

  public DistributedThreePhaseCommitCoordinator(String nodeName,
      long keepAliveTime,
 int opThreads,
      long wakeFrequency, DistributedCommitCoordinatorController controller,
      CoordinatorTaskBuilder builder) {
    super(controller, nodeName, keepAliveTime, opThreads, "commit-coordinator");
    this.controller = controller;
    setBuilder(builder);
  }

  public void setBuilder(CoordinatorTaskBuilder builder) {
    this.builder = builder;
  }

  /**
   * Kick off the named operation.
   * @param operationName name of the operation to start
   * @param operationInfo information for the operation to start
   * @param expectedNodes expected nodes to start
   * @return handle to the running operation, if it was started correctly, <tt>null</tt> otherwise
   * @throws RejectedExecutionException if there are no more available threads to run the operation
   */
  public CoordinatorTask kickOffCommit(String operationName, byte[] operationInfo,
      List<String> expectedNodes) throws RejectedExecutionException {
    // build the operation
    CoordinatorTask commit = builder.buildOperation(this, operationName, operationInfo,
      expectedNodes);
    if (this.submitOperation(commit.getErrorListener(), operationName, commit)) {
      return commit;
    }
    return null;
  }

  /**
   * Notification that the operation had another node finished preparing the running operation
   * @param operationName name of the operation that prepared
   * @param node name of the node that prepared
   */
  public void prepared(String operationName, final String node) {
    new NotifyListener(operationName) {
      @Override
      public void notifyOperation(CoordinatorTask task) {
        task.prepared(node);
      }
    }.run();
  }

  /**
   * Notification that the operation had another node finished committing the running operation
   * @param operationName name of the operation that finished
   * @param node name of the node that committed
   */
  public void committed(String operationName, final String node) {
    new NotifyListener(operationName) {
      @Override
      public void notifyOperation(CoordinatorTask task) {
        task.committed(node);
      }
    }.run();
  }

  /**
   * @return the controller for all current operations
   */
  public DistributedCommitCoordinatorController getController() {
    return controller;
  }
}