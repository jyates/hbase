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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.RemoteFailureException;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedThreePhaseCommitManager;

/**
 * Pass on operation changes to all commit members.
 * @param <L> Type of manager the controller will interact with
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface DistributedCommitController<L extends DistributedThreePhaseCommitManager<?, ?, ?>>
    extends Closeable {

  /**
   * Start running the controller. Must be called before running any other operations
   * @param manager manage progress for the operation - listen for errors, state change,etc.
   */
  public void start(L manager);

  /**
   * Notify the coordinator that we aborted the operation locally
   * @param operationName name of the operation that was aborted
   * @param cause the reason why the operation needs to be aborted
   * @throws IOException if the controller can't reach the other members of the operation (and can't
   *           recover).
   */
  public void abortOperation(String operationName, RemoteFailureException cause) throws IOException;
}