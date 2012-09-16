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
package org.apache.hadoop.hbase.server.snapshot.task;

import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorListener;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;

/**
 * General snapshot operation taken on a regionserver
 */
public abstract class SnapshotTask implements ExceptionCheckable<HBaseSnapshotException>, Runnable {

  protected final SnapshotDescription snapshot;
  protected final SnapshotErrorListener errorMonitor;

  /**
   * @param snapshot Description of the snapshot we are going to operate on
   * @param monitor listener interested in failures to the snapshot caused by this operation
   */
  public SnapshotTask(SnapshotDescription snapshot, SnapshotErrorListener monitor) {
    this.snapshot = snapshot;
    this.errorMonitor = monitor;
  }

  protected final void snapshotFailure(String message, Exception e) {
    this.errorMonitor.snapshotFailure(message, this.snapshot, e);
  }

  @Override
  public void failOnError() throws HBaseSnapshotException {
    this.errorMonitor.failOnError();
  }

  @Override
  public boolean checkForError() {
    return this.errorMonitor.checkForError();
  }
}
