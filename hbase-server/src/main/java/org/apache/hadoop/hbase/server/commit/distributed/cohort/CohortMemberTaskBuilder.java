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
package org.apache.hadoop.hbase.server.commit.distributed.cohort;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.server.commit.ThreePhaseCommit;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedCommitException;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedThreePhaseCommitErrorListener;

/**
 * Task builder to build cohort tasks for a {@link CohortMemberTaskRunner}. This is delegated to
 * when the {@link CohortMemberTaskRunner} is requests to
 * {@link CohortMemberTaskRunner#runNewOperation(String, byte[])}.
 * @see CohortMemberTaskRunner
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface CohortMemberTaskBuilder {

  /**
   * Build a {@link ThreePhaseCommit} operation when requested.
   * @param name name of the operation to start
   * @param data data passed from the coordinator about the operation
   * @return {@link ThreePhaseCommit} to run or <tt>null</tt> if the no operation should be run
   * @throws IllegalArgumentException if the operation could not be run because of errors in the
   *           request
   * @throws IllegalStateException if the current runner cannot accept any more new requests
   */
  public <T extends ThreePhaseCommit<? extends DistributedThreePhaseCommitErrorListener, DistributedCommitException>> T buildNewOperation(
      String name, byte[] data);
}