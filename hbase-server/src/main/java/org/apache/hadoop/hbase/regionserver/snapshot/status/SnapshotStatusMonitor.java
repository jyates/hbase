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
package org.apache.hadoop.hbase.regionserver.snapshot.status;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.regionserver.snapshot.status.SnapshotFailureMonitorImpl.SnapshotFailureMonitorFactory;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Simple top-level wrapper that just monitors other monitors
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SnapshotStatusMonitor extends SnapshotStatus implements SnapshotFailureMonitor {

  private final List<SnapshotStatus> progressStatus = Collections
      .synchronizedList(new LinkedList<SnapshotStatus>());
  private final SnapshotFailureMonitorImpl failureMonitor;

  public SnapshotStatusMonitor(SnapshotFailureMonitorImpl failureStatus,
      RegionSnapshotOperationStatus... snapshotStatus) {
    progressStatus.addAll(Arrays.asList(snapshotStatus));
    this.failureMonitor = failureStatus;
  }

  public void addStatus(SnapshotStatus status) {
    this.progressStatus.add(status);
  }

  @Override
  public boolean checkDone() throws SnapshotCreationException {
    // first check to make sure we didn't fail
    failureMonitor.checkFailure();
    // check each of the statuses
    for (SnapshotStatus status : progressStatus) {
      if (!status.isDone()) return false;
    }
    return true;
  }


  @Override
  public String getStatus() {
    StringBuilder sb = new StringBuilder("Snapshot status:\n");
    for (SnapshotStatus status : progressStatus) {
      sb.append(status.getStatus()+"\n");
    }
    return sb.toString();
  }

  @Override
  public void checkFailure() throws SnapshotCreationException {
    failureMonitor.checkFailure();
  }
  
  @Override
  public void localSnapshotFailure(SnapshotDescriptor snapshot, String description) {
    failureMonitor.localSnapshotFailure(snapshot, description);
  }

  /**
   * @param snapshot
   */
  public void remoteFailure(SnapshotDescriptor snapshot) {
    failureMonitor.remoteFailure(snapshot);
  }

  /**
   * Helper factory to create the snapshot monitor more simply
   */
  public static class Factory {

    private final SnapshotFailureMonitorFactory failureFactory;
    private final SnapshotDescriptor snapshot;

    public Factory(SnapshotFailureMonitorFactory failureFactory,
        SnapshotDescriptor snapshot) {
      this.failureFactory = failureFactory;
      this.snapshot = snapshot;
    }

    public SnapshotStatusMonitor create(long startTime){
      return new SnapshotStatusMonitor(failureFactory.createStatusMonitor(snapshot, startTime));
    }
  }
}
