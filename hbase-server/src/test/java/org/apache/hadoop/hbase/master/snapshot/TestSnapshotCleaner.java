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
package org.apache.hadoop.hbase.master.snapshot;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test general cleanup for snapshots after they complete/fail
 */
@Category(SmallTests.class)
public class TestSnapshotCleaner {

  /**
   * Test that that cleaner will not block when we wait on it to run in tests
   * @throws Exception on failure
   */
  @Test(timeout = 1000)
  public void testCleanClosesInTests() throws Exception {
    SnapshotCleaner.ensureCleanerRuns();
  }

  /**
   * Test when we run the cleaner multiple times with different directories that we don't have
   * concurrent access issues.
   * @throws Exception on failure
   */
  @Test(timeout = 1000)
  public void testOverlappingCleanup() throws Exception {
    final FileSystem fs = Mockito.mock(FileSystem.class);
    Path workingDir = new Path("working");
    final Configuration conf = new Configuration(false);
    // make sure we only wait 100ms to run the cleaner
    conf.setLong(SnapshotCleaner.CLEANUP_SNAPSHOT_WAIT_TIME_KEY, 100);
    // create the async task
    SnapshotCleaner.cleanupSnapshot(workingDir, fs, conf);
    SnapshotCleaner cleaner = SnapshotCleaner.current;

    // attempt to create a cleaner for the same directory
    SnapshotCleaner.cleanupSnapshot(workingDir, fs, conf);
    assertTrue("Created new cleaner for same directory!", cleaner == SnapshotCleaner.current);

    // now launch the cleaner
    Threads.setDaemonThreadRunning(cleaner, "test-cleaner");
    // this should block until the running cleaner is done
    final boolean[] done = new boolean[] { false };
    new Thread(new Runnable() {
      public void run() {
        SnapshotCleaner.cleanupSnapshot(new Path("other dir"), fs, conf);
        done[0] = true;
      }
    }).start();

    // while the snapshot cleaner is running, ensure that we haven't created a new cleaner
    while (SnapshotCleaner.running.availablePermits() == 0) {
      assertFalse("Create a new cleaner before done cleaning previous directory.", done[0]);
    }
  }
}