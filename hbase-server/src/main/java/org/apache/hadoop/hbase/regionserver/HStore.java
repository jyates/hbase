/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics.SchemaAware;

import com.google.common.collect.ImmutableList;

/**
 * Interface for objects that hold a column family in a Region. Its a memstore and a set of zero or
 * more StoreFiles, which stretch backwards over time.
 */
public interface HStore extends SchemaAware, HeapSize {

  // General Accessors
  public KeyValue.KVComparator getComparator();

  List<StoreFile> getStorefiles();

  ImmutableList<StoreFile> close() throws IOException;

  /**
   * Return a scanner for both the memstore and the HStore files. Assumes we are not in a
   * compaction.
   * @throws IOException
   */
  public StoreScanner getScanner(Scan scan, final NavigableSet<byte[]> targetCols)
      throws IOException;

  /**
   * Increments the value for the given row/family/qualifier. This function will always be seen as
   * atomic by other readers because it only puts a single KV to memstore. Thus no read/write
   * control necessary.
   * @param row
   * @param f
   * @param qualifier
   * @param newValue the new value to set into memstore
   * @return memstore size delta
   * @throws IOException
   */
  public long updateColumnValue(byte[] row, byte[] f, byte[] qualifier, long newValue)
      throws IOException;

  /**
   * Adds or replaces the specified KeyValues.
   * <p>
   * For each KeyValue specified, if a cell with the same row, family, and qualifier exists in
   * MemStore, it will be replaced. Otherwise, it will just be inserted to MemStore.
   * <p>
   * This operation is atomic on each KeyValue (row/family/qualifier) but not necessarily atomic
   * across all of them.
   * @param kvs
   * @return memstore size delta
   * @throws IOException
   */
  public long upsert(List<KeyValue> kvs) throws IOException;

  /**
   * Adds a value to the memstore
   * @param kv
   * @return memstore size delta
   */
  public long add(KeyValue kv);

  /**
   * Removes a kv from the memstore. The KeyValue is removed only if its key & memstoreTS matches
   * the key & memstoreTS value of the kv parameter.
   * @param kv
   */
  public void rollback(final KeyValue kv);

  /**
   * Find the key that matches <i>row</i> exactly, or the one that immediately precedes it. WARNING:
   * Only use this method on a table where writes occur with strictly increasing timestamps. This
   * method assumes this pattern of writes in order to make it reasonably performant. Also our
   * search is dependent on the axiom that deletes are for cells that are in the container that
   * follows whether a memstore snapshot or a storefile, not for the current container: i.e. we'll
   * see deletes before we come across cells we are to delete. Presumption is that the
   * memstore#kvset is processed before memstore#snapshot and so on.
   * @param row The row key of the targeted row.
   * @return Found keyvalue or null if none found.
   * @throws IOException
   */
  KeyValue getRowKeyAtOrBefore(final byte[] row) throws IOException;

  // Compaction oriented methods

  boolean throttleCompaction(long compactionSize);

  /**
   * getter for CompactionProgress object
   * @return CompactionProgress object; can be null
   */
  public CompactionProgress getCompactionProgress();

  public CompactionRequest requestCompaction() throws IOException;

  public CompactionRequest requestCompaction(int priority) throws IOException;

  public void finishRequest(CompactionRequest cr);

  /**
   * @return true if we should run a major compaction.
   */
  public boolean isMajorCompaction() throws IOException;

  public void triggerMajorCompaction();

  /**
   * See if there's too much store files in this store
   * @return true if number of store files is greater than the number defined in minFilesToCompact
   */
  public boolean needsCompaction();

  public int getCompactPriority();

  /**
   * @return The priority that this store should have in the compaction queue
   * @param priority
   */
  public int getCompactPriority(int priority);

  public StoreFlusher getStoreFlusher(long cacheFlushId);

  // Split oriented methods

  public boolean canSplit();

  /**
   * Determines if Store should be split
   * @return byte[] if store should be split, null otherwise.
   */
  public byte[] getSplitPoint();

  // Bulk Load methods

  /**
   * This throws a WrongRegionException if the HFile does not fit in this region, or an
   * InvalidHFileException if the HFile is not valid.
   */
  void assertBulkLoadHFileOk(Path srcPath) throws IOException;

  /**
   * This method should only be called from HRegion. It is assumed that the ranges of values in the
   * HFile fit within the stores assigned region. (assertBulkLoadHFileOk checks this)
   */
  void bulkLoadHFile(String srcPathStr) throws IOException;

  // General accessors into the state of the store
  // TODO abstract some of this out into a metrics class

  /**
   * @return <tt>true</tt> if the store has any underlying reference files to older HFiles
   */
  public boolean hasReferences();

  /**
   * @return The size of this store's memstore, in bytes
   */
  long getMemStoreSize();

  public HColumnDescriptor getFamily();

  /**
   * @return The maximum memstoreTS in all store files.
   */
  public long getMaxMemstoreTS();

  /**
   * @return the data block encoder
   */
  public HFileDataBlockEncoder getDataBlockEncoder();

  /**
   * @return the number of files in this store
   */
  public int getNumberOfStoreFiles();

  /** @return aggregate size of all HStores used in the last compaction */
  public long getLastCompactSize();

  /** @return aggregate size of HStore */
  public long getSize();

  /**
   * @return Count of store files
   */
  int getStorefilesCount();

  /**
   * @return The size of the store files, in bytes, uncompressed.
   */
  long getStoreSizeUncompressed();

  /**
   * @return The size of the store files, in bytes.
   */
  long getStorefilesSize();

  /**
   * @return The size of the store file indexes, in bytes.
   */
  long getStorefilesIndexSize();

  /**
   * Returns the total size of all index blocks in the data block indexes, including the root level,
   * intermediate levels, and the leaf level for multi-level indexes, or just the root level for
   * single-level indexes.
   * @return the total size of block indexes in the store
   */
  long getTotalStaticIndexSize();

  /**
   * Returns the total byte size of all Bloom filter bit arrays. For compound Bloom filters even the
   * Bloom blocks currently not loaded into the block cache are counted.
   * @return the total size of all Bloom filters in the store
   */
  long getTotalStaticBloomSize();

  // Test-helper methods

  /**
   * Compact the most recent N files. Used in testing.
   */
  public void compactRecentForTesting(int N) throws IOException;

  /**
   * Used for tests. Get the cache configuration for this Store.
   */
  public CacheConfig getCacheConfig();

  /**
   * @return the parent region hosting this store
   */
  public HRegion getHRegion();
}