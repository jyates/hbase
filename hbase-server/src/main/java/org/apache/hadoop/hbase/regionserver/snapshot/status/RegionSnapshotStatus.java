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

import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;

/**
 * Monitor status for the progress on a single region's snapshot
 */
public class RegionSnapshotStatus extends SnapshotStatus {

  private final RegionSnapshotOperationStatus parent;
  public RegionSnapshotStatus(RegionSnapshotOperationStatus parent) {
    this.parent = parent;
  }

  /**
   * Indicate that this region has become become stable (no new writes)
   */
  public void becomeStable() {
    parent.regionBecameStable();
  }

  @Override
  public String getStatus() {
    try {
      if (this.isDone()) return "Done!";
    } catch (SnapshotCreationException e) {
      return "snapshot creation exception!";
    }
    return "working...";
  }
}
