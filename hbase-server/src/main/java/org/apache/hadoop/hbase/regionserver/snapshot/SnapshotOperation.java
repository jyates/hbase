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
package org.apache.hadoop.hbase.regionserver.snapshot;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.snapshot.monitor.SnapshotErrorMonitor;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Operation that actually does all the work of taking a snapshot
 * per-regionserver
 */
public abstract class SnapshotOperation implements Runnable {

  private static final Log LOG = LogFactory.getLog(SnapshotOperation.class);
  protected final SnapshotErrorMonitor errorMonitor;
  private final SnapshotFailureListener failureListener;
  protected final SnapshotDescriptor snapshot;

  public SnapshotOperation(SnapshotErrorMonitor monitor, SnapshotFailureListener listener,
      SnapshotDescriptor snapshot) {
    this.errorMonitor = monitor;
    this.failureListener = listener;
    this.snapshot = snapshot;
  }

  protected void failSnapshot(String reason, Throwable t) {
    LOG.error("Failing snapshot becuase:" + reason, t);
    failureListener.snapshotFailure(snapshot, reason);
  }


  /**
   * @see SnapshotFailureMonitor#checkFailure()
   * @throws SnapshotCreationException
   */
  protected void checkFailure() throws SnapshotCreationException {
    this.errorMonitor.checkForError(this.getClass());
  }

}