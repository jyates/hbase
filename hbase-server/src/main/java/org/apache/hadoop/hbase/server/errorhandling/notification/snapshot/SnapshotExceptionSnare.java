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
package org.apache.hadoop.hbase.server.errorhandling.notification.snapshot;

import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.errorhandling.notification.CheckedErrorSnare;
import org.apache.hadoop.hbase.server.errorhandling.notification.ErrorSnare;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.UnexpectedSnapshotFailureException;

/**
 * An {@link ErrorSnare} that only returns {@link HBaseSnapshotException} on calls to
 * {@link #failOnException()}. This is shared between all subtasks of a given snapshot task,
 * ensuring that if any subtask fails (and passes in the exception to
 * {@link #snapshotFailure(HBaseSnapshotException)}), then all subtasks will find the exception and
 * be able to kill themselves quickly.
 * <p>
 * This can also be attached to a Timer allowing the snapshot task to be timed and automatically
 * fail all subtasks when the timer has expired.
 */
public class SnapshotExceptionSnare extends CheckedErrorSnare<HBaseSnapshotException> {

  private SnapshotDescription snapshot;

  SnapshotExceptionSnare(SnapshotDescription snapshot) {
    this.snapshot = snapshot;
  }

  @Override
  protected HBaseSnapshotException map(Exception e) {
    if (e instanceof HBaseSnapshotException) return (HBaseSnapshotException) e;
    return new UnexpectedSnapshotFailureException(e, snapshot);
  }

  /**
   * Fail the current snapshot due to the given exception.
   * @param cause
   */
  public void snapshotFailure(HBaseSnapshotException cause) {
    this.handleNotification(SnapshotFailureNotificationUtils.mapLocalSnapshotFailure(this, cause),
      null);
  }

}
