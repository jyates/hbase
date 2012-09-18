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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotErrorMonitorFactory;
import org.apache.hadoop.hbase.server.snapshot.errorhandling.SnapshotExceptionDispatcher;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(SmallTests.class)
public class TestRegionSnapshotOperation {

  @Test
  public void testRunOnlyWaitsForPrepare() throws InterruptedException, IOException {
    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName("snapshot").build();
    HRegion region = Mockito.mock(HRegion.class);
    Mockito.when(region.getRegionNameAsString()).thenReturn("Some region");
    SnapshotExceptionDispatcher dispatcher = new SnapshotErrorMonitorFactory()
        .createGenericSnapshotErrorMonitor();
    RegionSnapshotOperationStatus monitor = new RegionSnapshotOperationStatus(1, 1000);
    final boolean[] commitSnapshot = new boolean[] { false };
    final boolean[] finishSnapshot = new boolean[] { false };
    RegionSnapshotOperation op = new RegionSnapshotOperation(snapshot, region, dispatcher, 1000,
        monitor) {

      @Override
      public void prepare() throws HBaseSnapshotException {
        // NOOP
      }

      @Override
      public void commitSnapshot() {
        commitSnapshot[0] = true;
      }

      @Override
      public void finishSnapshot() {
        finishSnapshot[0] = true;
      }

      @Override
      public void cleanup(Exception e) {
        // NOOP
      }
    };
    CountDownLatch prepared = op.getPreparedLatch();
    CountDownLatch committed = op.getCommitFinishedLatch();
    op.run();
    assertEquals("prepared latch didn't count down at end of op run", 0, prepared.getCount());
    assertEquals("committed latch counted down unexpectedly", 1, committed.getCount());
    op.getAllowCommitLatch().countDown();
    // wait for the commit phase to complete
    committed.await();
    assertTrue("Operation committed, but didn't run commitSnapshot()", commitSnapshot[0]);
    // ensure that we add the region to the snapshot on commit
    Mockito.verify(region, Mockito.times(1)).addRegionToSnapshot(snapshot, op);

    // wait for the finish phase to complete
    op.getCompletedLatch().await();
    assertTrue("Operation finished, but didn't run finishSnapshot()", commitSnapshot[0]);

    Mockito.verify(region, Mockito.times(1)).getRegionNameAsString();
    Mockito.verifyNoMoreInteractions(region);
  }

}
