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
package org.apache.hadoop.hbase.server.exceptionhandling.snapshot;

import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.errorhandling.WeakReferencingNotificationBroadcasterSupport;

/**
 * Factory to build exception monitors for a snapshot.
 * <p>
 */
public class SnapshotExceptionMonitorFactory {

  private WeakReferencingNotificationBroadcasterSupport broadcaster = new WeakReferencingNotificationBroadcasterSupport();

  public SnapshotExceptionSnare getNewSnapshotSnare(SnapshotDescription snapshot) {
    SnapshotExceptionSnare snare = new SnapshotExceptionSnare(snapshot);
    // snares have no filter - all errors are accepted
    this.broadcaster.addNotificationListener(snare, null, null);
    return snare;
  }

  public WeakReferencingNotificationBroadcasterSupport getBroadcaster() {
    return this.broadcaster;
  }
}
