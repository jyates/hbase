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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.server.commit.ThreePhaseCommit;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedThreePhaseCommitErrorDispatcher;
import org.apache.hadoop.hbase.server.commit.distributed.cohort.CohortMemberTaskBuilder;
import org.apache.hadoop.hbase.server.errorhandling.OperationAttemptTimer;

/**
 * Builder for a {@link ThreePhaseCommit} operation to run a distributed three-phase commit
 * coordinator. Subclasses should override
 * {@link #buildOperation(DistributedThreePhaseCommitCoordinator, String, byte[], List)} to get
 * custom commit functionality.
 * <p>
 * Similar to the {@link CohortMemberTaskBuilder}, but for the coordinator of the commit operation
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CoordinatorTaskBuilder {
  protected final Object[] timeoutInfo;
  protected final long timeout;
  protected final long wake;

  /**
   * @param wakeFreq frequency the task should check for errors
   * @param timeout max amount of time the task should run before failing. See
   *          {@link OperationAttemptTimer}
   * @param timeoutInfo information to pass along in the case of a timeout
   */
  public CoordinatorTaskBuilder(long wakeFreq, long timeout, Object[] timeoutInfo) {
    this.wake = wakeFreq;
    this.timeout = timeout;
    this.timeoutInfo = timeoutInfo;
  }

  /**
   * Build a coordinator operation for a distributed three phase commit
   * @param parent parent running the operation
   * @param operationName name of the operation (must be unique)
   * @param operationInfo information about the task to pass to cohort members
   * @param expectedNodes names of the expected cohort members
   * @return a {@link CoordinatorTask} that is ready to run the task
   */
  public CoordinatorTask buildOperation(DistributedThreePhaseCommitCoordinator parent,
      String operationName, byte[] operationInfo, List<String> expectedNodes) {
    DistributedThreePhaseCommitErrorDispatcher errorMonitor = new DistributedThreePhaseCommitErrorDispatcher();
    return new CoordinatorTask(parent, parent.getController(), errorMonitor, wake, timeout,
        timeoutInfo, operationName, operationInfo, expectedNodes);
  }
}