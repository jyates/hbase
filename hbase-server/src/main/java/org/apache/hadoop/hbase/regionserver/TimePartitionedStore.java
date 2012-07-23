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

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.Store.StoreFlusherImpl;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaConfigured;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Store that partitions writes between primary memstore and the memstore that will be flushed in
 * the timestamp-based snapshot. Writes that are earlier than the partition point are written to the
 * snapshot store, while those later are written to the primary memstore.
 */
public class TimePartitionedStore extends SchemaConfigured implements HStore {

  private final Store primary;
  private final Store snapshot;
  private final long splitPoint;

  private final Predicate<KeyValue> inSnapshot = new Predicate<KeyValue>() {
    @Override
    public boolean apply(KeyValue input) {
      return input.getTimestamp() <= splitPoint;
    }
  };

  private final Predicate<KeyValue> inFuture = new Predicate<KeyValue>() {
    @Override
    public boolean apply(KeyValue input) {
      return !inSnapshot.apply(input);
    }
  };

  public TimePartitionedStore(Store store, long timestamp) throws IOException {
    super(store.getHRegion().conf, store.getTableName(), store.getFamily().getNameAsString());
    this.primary = store;
    this.splitPoint = timestamp;
    this.snapshot = Store.snapshotAndClone(store);
  }

  @Override
  public long add(KeyValue kv) {
    if (inSnapshot.apply(kv)) return snapshot.add(kv);
    return primary.add(kv);
  }

  @Override
  public void rollback(KeyValue kv) {
    if (inSnapshot.apply(kv)) {
      snapshot.rollback(kv);
    } else {
      primary.rollback(kv);
    }
  }

  @Override
  public long upsert(Iterable<KeyValue> kvs) throws IOException {
    long ret = 0;
    ret += snapshot.upsert(Iterables.filter(kvs, inSnapshot));
    ret += primary.upsert(Iterables.filter(kvs, inFuture));
    return ret;
  }

  @Override
  public long getMemStoreSize() {
    return primary.getMemStoreSize() + snapshot.getMemStoreSize();
  }

  /**
   * @return current size of the memstore in the snapshot
   */
  public long getSnapshotMemStoreSize() {
    return this.snapshot.getMemStoreSize();

  }

  @Override
  public StoreFlusher getStoreFlusher(long cacheFlushId) {
    return new SnapshotStoreFlusher(this.primary.new StoreFlusherImpl(cacheFlushId),
        snapshot.memstore);
  }

  /**
   * @return the underlying delegate to the non-snapshot memstore
   */
  public HStore getDelgate() {
    return this.primary;
  }

  private class SnapshotStoreFlusher implements StoreFlusher {
    private StoreFlusherImpl delegate;

    private SnapshotStoreFlusher(StoreFlusherImpl delegate, MemStore snapshot) {
      this.delegate = delegate;
      delegate.snapshot = snapshot.kvset;
      delegate.snapshotTimeRangeTracker = snapshot.timeRangeTracker;
    }

    @Override
    public void prepare() {
      throw new UnsupportedOperationException("Snapshot StoreFlusher is already prepared, "
          + "so prepare() should never be called on it.");
    }

    @Override
    public void flushCache(MonitoredTask status) throws IOException {
      delegate.flushCache(status);
    }

    @Override
    public boolean commit(MonitoredTask status) throws IOException {
      return delegate.commit(status);
    }
  }

  // ---------------------------------------------------------------------------
  // Below here we delegate to the regular memstore scanner for seemingly
  // 'merge' operations (between the snapshot and current store), but that's
  // okay since the Store already merges between its stored snapshot and regular
  // memstore (and we are only passing pointers around).
  // ---------------------------------------------------------------------------

  @Override
  public StoreScanner getScanner(Scan scan, NavigableSet<byte[]> targetCols) throws IOException {
    return primary.getScanner(scan, targetCols);
  }

  @Override
  public KeyValue getRowKeyAtOrBefore(byte[] row) throws IOException {
    return primary.getRowKeyAtOrBefore(row);
  }

  @Override
  public long updateColumnValue(byte[] row, byte[] f, byte[] qualifier, long newValue)
      throws IOException {
    return primary.updateColumnValue(row, f, qualifier, newValue);
  }

  @Override
  public ImmutableList<StoreFile> close() throws IOException {
    return primary.close();
  }

  @Override
  public HColumnDescriptor getFamily() {
    return primary.getFamily();
  }

  @Override
  public long getMaxMemstoreTS() {
    return primary.getMaxMemstoreTS();
  }

  @Override
  public HFileDataBlockEncoder getDataBlockEncoder() {
    return primary.getDataBlockEncoder();
  }

  @Override
  public void compactRecentForTesting(int N) throws IOException {
    primary.compactRecentForTesting(N);

  }

  @Override
  public CompactionProgress getCompactionProgress() {
    return primary.getCompactionProgress();
  }

  @Override
  public CompactionRequest requestCompaction() throws IOException {
    return primary.requestCompaction();
  }

  @Override
  public CompactionRequest requestCompaction(int priority) throws IOException {
    return primary.requestCompaction(priority);
  }

  @Override
  public void finishRequest(CompactionRequest cr) {
    primary.finishRequest(cr);
  }

  @Override
  public int getNumberOfStoreFiles() {
    return primary.getNumberOfStoreFiles();
  }

  @Override
  public boolean canSplit() {
    return primary.canSplit();
  }

  @Override
  public byte[] getSplitPoint() {
    return primary.getSplitPoint();
  }

  @Override
  public long getLastCompactSize() {
    return primary.getLastCompactSize();
  }

  @Override
  public long getSize() {
    return primary.getLastCompactSize();
  }

  @Override
  public void triggerMajorCompaction() {
    primary.triggerMajorCompaction();
  }

  @Override
  public int getCompactPriority() {
    return primary.getCompactPriority();
  }

  @Override
  public int getCompactPriority(int priority) {
    return primary.getCompactPriority(priority);
  }

  @Override
  public boolean needsCompaction() {
    return primary.needsCompaction();
  }

  @Override
  public CacheConfig getCacheConfig() {
    return primary.getCacheConfig();
  }

  @Override
  public long heapSize() {
    return primary.heapSize();
  }

  @Override
  public KVComparator getComparator() {
    return primary.getComparator();
  }

  @Override
  public List<StoreFile> getStorefiles() {
    return primary.getStorefiles();
  }

  @Override
  public boolean throttleCompaction(long compactionSize) {
    return primary.throttleCompaction(compactionSize);
  }

  @Override
  public boolean isMajorCompaction() throws IOException {
    return primary.isMajorCompaction();
  }

  @Override
  public void assertBulkLoadHFileOk(Path srcPath) throws IOException {
    primary.assertBulkLoadHFileOk(srcPath);
  }

  @Override
  public void bulkLoadHFile(String srcPathStr) throws IOException {
    primary.bulkLoadHFile(srcPathStr);
  }

  @Override
  public boolean hasReferences() {
    return primary.hasReferences();
  }

  @Override
  public int getStorefilesCount() {
    return primary.getStorefilesCount();
  }

  @Override
  public long getStoreSizeUncompressed() {
    return primary.getStoreSizeUncompressed();
  }

  @Override
  public long getStorefilesSize() {
    return primary.getStorefilesSize();
  }

  @Override
  public long getStorefilesIndexSize() {
    return primary.getStorefilesIndexSize();
  }

  @Override
  public long getTotalStaticIndexSize() {
    return primary.getTotalStaticIndexSize();
  }

  @Override
  public long getTotalStaticBloomSize() {
    return primary.getTotalStaticBloomSize();
  }

  @Override
  public HRegion getHRegion() {
    return primary.getHRegion();
  }
}
