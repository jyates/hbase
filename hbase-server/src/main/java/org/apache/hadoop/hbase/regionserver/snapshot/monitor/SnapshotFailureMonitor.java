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
package org.apache.hadoop.hbase.regionserver.snapshot.monitor;

import org.apache.hadoop.hbase.regionserver.snapshot.SnapshotFailureListener;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Simple helper class that just delegates the error checking to a
 * {@link SnapshotErrorMonitor} and failure notifications to a
 * {@link SnapshotFailureListener}.
 */
public class SnapshotFailureMonitor implements SnapshotErrorMonitor, SnapshotFailureListener {

  private SnapshotErrorMonitor errorMonitor;
  private SnapshotFailureListener listener;

  public SnapshotFailureMonitor(SnapshotErrorMonitor errorMonitor, SnapshotFailureListener listener) {
    this.errorMonitor = errorMonitor;
    this.listener = listener;
  }

  @Override
  public void snapshotFailure(SnapshotDescriptor snapshot, String description) {
    listener.snapshotFailure(snapshot, description);
  }

  @Override
  public <T> boolean checkForError(Class<T> clazz) {
    return errorMonitor.checkForError(clazz);
  }

}
