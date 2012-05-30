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

import org.apache.hadoop.hbase.regionserver.snapshot.status.GlobalSnapshotFailureListener;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Listen for snapshot events
 */
public interface SnapshotListener extends GlobalSnapshotFailureListener {

  /**
   * Start the snapshot on the server
   * @param snapshot description of the snapshot to start
   * @return <tt>true</tt> if the listener is interested in further
   *         notifications about the passed snapshot, <tt>false</tt> otherwise
   */
  public void startSnapshot(SnapshotDescriptor snapshot);

  /**
   * The status of the passed snapshot has changed
   * @param snapshotName snapshot status that has changed
   */
  public void finishSnapshot(String snapshotName);
}
