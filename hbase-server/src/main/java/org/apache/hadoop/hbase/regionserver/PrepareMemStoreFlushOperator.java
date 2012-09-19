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
package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Setup a simple flush on all the stores. Runs before
 * {@link HRegion#finishFlushcache(List, long, org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl.WriteEntry, HLog, long, long, long, MonitoredTask)}
 */
@SuppressWarnings("javadoc")
public class PrepareMemStoreFlushOperator extends LockRegionOperator<Exception> {
  /** Total size of the flush */
  long flushSize = 0;
  /** {@link StoreFlusher} for each {@link HStore} */
  final List<Pair<StoreFlusher, Boolean>> flushers;
  /** MVCC write number for the flush */
  MultiVersionConsistencyControl.WriteEntry w = null;
  /** {@link HLog} flush start sequence number */
  long sequenceId;
  /** Sequence number of the completed flush */
  long completeSequenceId;

  /** mvcc for the region */
  private final MultiVersionConsistencyControl mvcc;
  /** flush status to update */
  private final MonitoredTask status;
  /** sequence id to use incase one cannot be found from the hlog */
  private final long mysequenceId;
  /** WAL from the region to write flush info */
  private final HLog wal;

  public PrepareMemStoreFlushOperator(HRegion region, MonitoredTask status, long myseqid,
      MultiVersionConsistencyControl mvcc) {
    super(region);
    this.status = status;
    this.mysequenceId = myseqid;
    this.wal = region.log;
    this.mvcc = mvcc;
    this.flushers = new ArrayList<Pair<StoreFlusher, Boolean>>(region.getStores().size());
  }

  @Override
  public void prepare() {
    // lock the region
    lockRegion();

    // get the flushsize
    this.flushSize = region.memstoreSize.get();

    // roll forward the mvcc
    this.w = mvcc.beginMemstoreInsert();
    mvcc.advanceMemstore(w);

    status.setStatus("Preparing to flush memstore by snapshotting stores");
    sequenceId = (wal == null) ? mysequenceId : wal.startCacheFlush(region.getRegionInfo()
        .getEncodedNameAsBytes());
    completeSequenceId = region.getCompleteCacheFlushSequenceId(sequenceId);
  }

  @Override
  public void operateOnStore(Store store) {
    StoreFlusher flusher = store.getStoreFlusher(completeSequenceId);
    this.flushers.add(new Pair<StoreFlusher, Boolean>(flusher, store instanceof HStore));
  }

  @Override
  public final void finish() {
    for (Pair<StoreFlusher, Boolean> flusher : flushers) {
      // if it is from a Store we can run prepare
      if (flusher.getSecond()) {
        flusher.getFirst().prepare();
      }
      this.flushSize += flusher.getFirst().getFlushSize().get();
    }
    unlockRegion();
  }
}