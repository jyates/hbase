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

import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Helper class that just notifies the subclass if the snapshot failure is the
 * one it was expecting.
 */
public abstract class BoundSnapshotFailureListener implements SnapshotFailureListener {

  protected SnapshotDescriptor snapshot;

  public BoundSnapshotFailureListener(SnapshotDescriptor snapshot) {
    this.snapshot = snapshot;
  }

  @Override
  public void snapshotFailure(SnapshotDescriptor snapshot, String description) {
    if (this.snapshot.equals(snapshot)) this.snapshotFailure(description);

  }

  /**
   * Notification that the snapshot has failed.
   * @param description reason why the snapshot failed
   */
  protected abstract void snapshotFailure(String description);

}
