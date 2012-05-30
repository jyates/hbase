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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class WALSnapshotMonitor extends SnapshotStatus {

  private static final Log LOG = LogFactory.getLog(WALSnapshotMonitor.class);
  private final int expected;
  private volatile int completed;

  public WALSnapshotMonitor(int expectedWALs) {
    this.expected = expectedWALs;
  }

  public void completedFile() {
    this.completed++;
  }

  @Override
  public String getStatus() {
    return "Completed referencing" + completed + " of " + expected + " WAL files.";
  }

  @Override
  public boolean checkDone() {
    if (completed >= expected) {
      LOG.debug("WAL has been sucessfully added to the snapshot.");
      return true;
    }
    return false;
  }
}