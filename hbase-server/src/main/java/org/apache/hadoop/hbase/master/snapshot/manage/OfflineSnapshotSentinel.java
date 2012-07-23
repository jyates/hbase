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
package org.apache.hadoop.hbase.master.snapshot.manage;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.handler.DisabledTableSnapshotHandler;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * {@link SnapshotSentinel} to run/monitor the snapshot of an 'offline'/disabled table.
 */
public class OfflineSnapshotSentinel extends SnapshotSentinel {
  private static final Log LOG = LogFactory.getLog(OnlineSnapshotSentinel.class);
  private final AtomicBoolean isDone;
  private final ExecutorService executor;
  private MasterServices master;
  private Server server;

  /**
   * @param hsd descriptor for the snapshot to take. Doesn't matter if this is Timestamp or Globally
   *          consistent - since the table is offline, all the files are kept around giving point in
   *          time
   * @param master parent running the snapshot
   * @param isSnapshotDone global indicator to check to progress of the snapshot
   * @param executorService executor to run the snapshot on
   * @throws IOException
   */
  OfflineSnapshotSentinel(SnapshotDescriptor hsd, MasterServices master, Server server,
      AtomicBoolean isSnapshotDone, ExecutorService executorService) throws IOException {
    super(hsd, master);
    this.isDone = isSnapshotDone;
    this.executor = executorService;
    this.master = master;
    this.server  = server;
  }

  @Override
  public void snapshot() throws IOException {
    // setup the snapshot

    // submit the snapshot operation
    this.executor.submit(new DisabledTableSnapshotHandler(this.getSnapshot(), this.server,
        this.master, this));
    LOG.debug("Waiting for disabled table snapshot to complete");

    // wait for the snapshot to complete
    while (!isDone.get()) {
      synchronized (isDone) {
        try {
          isDone.wait(wakeFrequency);
          failOnError();
        } catch (InterruptedException e) {
          LOG.debug("Interruped while waiting for snapshot to complete - ignoring!");
        }
      }
    }
    LOG.debug("Snapshot completed on master, returning successfully.");
  }
}
