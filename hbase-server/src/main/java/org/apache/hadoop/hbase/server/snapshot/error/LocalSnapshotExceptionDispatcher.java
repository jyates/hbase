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
package org.apache.hadoop.hbase.server.snapshot.error;

import org.apache.hadoop.hbase.protobuf.generated.DistributedCommitProtos.CommitPhase;
import org.apache.hadoop.hbase.server.commit.TwoPhaseCommitErrorListener;
import org.apache.hadoop.hbase.server.commit.TwoPhaseCommitable;
import org.apache.hadoop.hbase.server.errorhandling.impl.delegate.DelegatingExceptionDispatcher;
import org.apache.hadoop.hbase.server.snapshot.errorhandling.SnapshotExceptionDispatcher;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;

/**
 * Single-use snapshot exception dispatcher for sub-tasks of a snapshot that are
 * {@link TwoPhaseCommitable}.
 * <p>
 * Handles propagating local and remote exceptions as snapshot failures
 */
public class LocalSnapshotExceptionDispatcher
    extends
    DelegatingExceptionDispatcher<SnapshotExceptionDispatcher, SnapshotFailureListener, HBaseSnapshotException>
    implements TwoPhaseCommitErrorListener<HBaseSnapshotException> {

  /**
   * @param delegate delegate all error calls to the passed delegate
   */
  public LocalSnapshotExceptionDispatcher(SnapshotExceptionDispatcher delegate) {
    super(delegate);
  }

  @Override
  public void localOperationException(CommitPhase phase, HBaseSnapshotException cause) {
    this.delegate.receiveError("Failed task during:" + phase, cause, phase);
  }
}
