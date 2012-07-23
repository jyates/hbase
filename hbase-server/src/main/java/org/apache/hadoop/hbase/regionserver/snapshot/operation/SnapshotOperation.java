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
package org.apache.hadoop.hbase.regionserver.snapshot.operation;

import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotErrorCheckable;
import org.apache.hadoop.hbase.snapshot.monitor.SnapshotFailureListener;

/**
 * General snapshot operation taken on a regionserver
 */
public abstract class SnapshotOperation extends SnapshotErrorCheckable implements Runnable {

  /**
   * @param snapshot Description of the snapshot we are going to operate on
   */
  public SnapshotOperation(SnapshotDescriptor snapshot) {
    super(snapshot);
  }

  /**
   * @param snapshot Description of the snapshot we are going to operate on
   * @param listener listener interested in failures to the snasphot caused by this operation
   */
  public SnapshotOperation(SnapshotDescriptor snapshot, SnapshotFailureListener listener) {
    super(snapshot, listener);
  }

}
