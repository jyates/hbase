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
package org.apache.hadoop.hbase.server.commit;

import org.apache.hadoop.hbase.protobuf.generated.DistributedCommitProtos.CommitPhase;

/**
 * Listen for errors to a running {@link TwoPhaseCommitable}
 * @param <E> Type of exception the running operation will throw
 */
public interface TwoPhaseCommitErrorListener<E extends Exception> {

  /**
   * The {@link TwoPhaseCommitable} operation failed at the given phase
   * @param phase phase where the operation failed
   * @param cause the cause of the failure
   */
  public void localOperationException(CommitPhase phase, E cause);
}
