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
package org.apache.hadoop.hbase.snapshot.monitor;

import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;

/**
 * Monitor to check to see if a snapshot has failed. Generally should be used in aggregate with
 * other {@link SnapshotErrorMonitor SnapshotErrorMonitors} from different parts of the snapshot to
 * develop a global view of the snapshot error.
 * @see FailureMonitorFactory
 */
public interface SnapshotErrorMonitor {

  /**
   * Check for an error in a snapshot
   * @return <tt>true</tt> if an error exists, <tt>false</tt> otherwise
   */
  public boolean checkForError();

  /**
   * Check to see if the snapshot has failed. Returns cleanly if the snapshot should preceed.
   * @throws SnapshotCreationException thrown if the snapshot has failed and should be aborted
   *           globally (if not already)
   */
  public void failOnError() throws SnapshotCreationException;
}
