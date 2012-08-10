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
package org.apache.hadoop.hbase.server.commit.distributed.controller;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.server.commit.distributed.coordinator.DistributedThreePhaseCommitCoordinator;

/**
 * Controller for the coordinator to run a distributed two-phase commit
 * @see DistributedThreePhaseCommitCoordinator
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface DistributedCommitCoordinatorController extends
    DistributedCommitController<DistributedThreePhaseCommitCoordinator> {

  /**
   * Start the two phase commit
   * @param operationName name of the commit to start
   * @param info information that should be passed to all cohorts
   * @param cohort names of the members of the cohort to expect to reach the prepare phase
   * @throws IllegalArgumentException if the operation was already marked as failed in zookeeper
   * @throws IOException if we can't reach the remote notification mechanism
   */
  public void prepareOperation(String operationName, byte[] info, List<String> cohort)
      throws IOException, IllegalArgumentException;

  /**
   * Start the commit phase. Must come after calling {@link #prepareOperation(String, byte[], List)}
   * .
   * @param operationName name of the operation to start
   * @param cohort cohort members to expect to join the commit phase
   * @throws IOException if we can't reach the remote notification mechanism
   */
  public void commitOperation(String operationName, List<String> cohort) throws IOException;

  /**
   * Reset the distributed state for operation
   * @param operationName name of the operation to reset
   * @throws IOException if the remote notification mechanism cannot be reached
   */
  public void resetOperation(String operationName) throws IOException;
}
