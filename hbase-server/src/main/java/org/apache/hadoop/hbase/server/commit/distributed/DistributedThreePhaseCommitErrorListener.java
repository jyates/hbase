/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.DistributedCommitProtos.CommitPhase;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.RemoteFailureException;
import org.apache.hadoop.hbase.server.commit.ThreePhaseCommit;
import org.apache.hadoop.hbase.server.commit.ThreePhaseCommitErrorListenable;
import org.apache.hadoop.hbase.server.commit.distributed.controller.DistributedCommitControllerErrorListener;

/**
 * Error listener for an operation that is running a distributed {@link ThreePhaseCommit} and
 * therefore must distinguish between remote and local errors as well as connection errors.
 * @see DistributedCommitException
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface DistributedThreePhaseCommitErrorListener extends
    ThreePhaseCommitErrorListenable<DistributedCommitException>,
    DistributedCommitControllerErrorListener {

  /**
   * Notification that a remote failure caused the operation to fail.
   * @param remoteCause the root cause of the failure
   */
  public void remoteCommitError(RemoteFailureException remoteCause);

  /**
   * Notification that a local error caused the operation to fail
   * @param phase the phase during which the failure occurred
   * @param cause the local, root cause of the failure
   */
  public void localOperationException(CommitPhase phase, DistributedCommitException cause);

  /**
   * Add an error listener to listen for all the same errors <tt>this</tt> receives.
   * @param listener listener to listen for errors
   */
  public void addErrorListener(DistributedThreePhaseCommitErrorListener listener);
}