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
package org.apache.hadoop.hbase.master.snapshot.monitor;

import org.apache.hadoop.hbase.master.snapshot.manage.SnapshotSentinel;

/**
 * Listen for progress/status of a given snapshot.
 * <p>
 * Tied to a single snapshot and {@link SnapshotSentinel}
 */
public interface MasterSnapshotStatusListener {

  /**
   * Notification that all servers are prepared to take a snapshot
   */
  public void allServersPreparedSnapshot();

  /**
   * Idempotent operation to indicate that a region server aborted a snapshot
   * @param rsID id of the region server in the snapshot
   */
  public void rsAbortedSnapshot(String rsID);

  /**
   * Idempotent operation to indicate that a region server is prepared to take a snapshot
   * @param rsID id of the region server in the snapshot
   */
  public void rsJoinedSnapshot(String rsID);

  /**
   * Idempotent operation to indicate that a region server has successfully completed (committed) a
   * snapshot
   * @param rsID id of the region server in the snapshot
   */
  public void rsCompletedSnapshot(String rsID);

}
