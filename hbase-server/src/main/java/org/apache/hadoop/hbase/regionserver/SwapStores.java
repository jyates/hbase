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
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Simple store operation that just swaps the regions current stores for the stores specified
 * throughout the course of the operation
 * @param <E> type of exception that can be thrown from the operation
 */
public abstract class SwapStores<E extends Exception> extends LockRegionOperator<E> {

  protected Map<byte[], Store> storesToSwapIn = new TreeMap<byte[], Store>(
      Bytes.BYTES_RAWCOMPARATOR);

  public SwapStores(HRegion parent) {
    super(parent);
  }

  @Override
  public void finish() {
    // swap back the stores
    region.stores.clear();
    region.stores.putAll(storesToSwapIn);

    // unlock the region
    unlockRegion();
  }

  /**
   * Swap out the current stores for a TimePartitioned store that is immediately ready to take a
   * snapshot.
   */
  public static class SwapForTimeParitionedStore extends SwapStores<IOException> {
    private static final Log LOG = LogFactory.getLog(SwapForTimeParitionedStore.class);

    long sequenceId;

    private long splitPoint;
    private final MonitoredTask status;
    private final HLog log;

    public SwapForTimeParitionedStore(HRegion parent, MonitoredTask status, long splitPoint) {
      super(parent);
      this.splitPoint = splitPoint;
      this.log = parent.log;
      this.status = status;
    }

    @Override
    public void prepare() {
      // lock the region
      String msg = "Obtaining lock to block concurrent updates for timestamp snapshot";
      LOG.debug(msg);
      status.setStatus(msg);
      lockRegion();

      status.setStatus("Preparing to timestamp snapshot by swapping stores");
      // if there is a log, start flushing
      // note that startCacheFlush takes a lock, so we need to make sure we release the lock when we
      // restore the stores
      sequenceId = this.log == null ? -1 : this.log.startCacheFlush(region.getRegionInfo()
          .getEncodedNameAsBytes());
    }

    @Override
    public void operateOnStore(Store store) throws IOException {
      if (!(store instanceof HStore)) {
        LOG.debug("Can't swap stores for a store that isn't a HStore, but is:" + store);
        return;
      }
      // wrapping the existing stores with the snapshot/funneling delegator
      // this is equivalent to storeFlusher.prepare in internalFlushcache
      storesToSwapIn.put(store.getFamily().getName(), new TimePartitionedStore((HStore) store,
          splitPoint));
      LOG.debug("Replacing store:" + store + " with TimePartitioned store splitting at:"
          + DateFormat.getDateTimeInstance().format(new Date(splitPoint)));
    }
  }

  /**
   * Swap each of the {@link TimePartitionedStore} for the original stores, after getting the store
   * ready to flush. Actual flush takes place in the HRegion and deals with unlocking the hlog too.
   */
  public static class FlushAndSwapBack extends SwapStores<IOException> {
    private static final Log LOG = LogFactory.getLog(FlushAndSwapBack.class);

    // values to pass back to calling hregion
    MultiVersionConsistencyControl.WriteEntry w = null;
    long completeSequenceId;
    long flushSize = 0;
    final List<StoreFlusher> flushers;

    // sequence id for the log
    private final MultiVersionConsistencyControl mvcc;
    private final long sequenceId;

    public FlushAndSwapBack(HRegion parent, MultiVersionConsistencyControl mvcc, long sequenceId) {
      super(parent);
      this.sequenceId = sequenceId;
      this.mvcc = mvcc;
      this.flushers = new ArrayList<StoreFlusher>(parent.getStores().size());
    }

    @Override
    public void prepare() {
      // lock the region
      lockRegion();

      // roll forward the mvcc
      LOG.debug("Rolling forward mvcc");
      this.w = mvcc.beginMemstoreInsert();
      mvcc.advanceMemstore(w);

      // get the wal sequence id
      completeSequenceId = region.getCompleteCacheFlushSequenceId(sequenceId);
    }

    @Override
    public void operateOnStore(Store store) {
      // get the store flusher
      this.flushers.add(store.getStoreFlusher(completeSequenceId));

      // get the original store to swap back
      if (store instanceof TimePartitionedStore) {
        LOG.debug("Swapping back original store...");
        TimePartitionedStore tps = (TimePartitionedStore) store;
        // set the store to be the delegate (original store)
        store = tps.getDelgate();
        // get the flushize of the store that was swapped back
        flushSize += tps.getSnapshotMemStoreSize();
      }
      storesToSwapIn.put(store.getFamily().getName(), store);
    }
  }

  /**
   * Swap any {@link TimePartitionedStore} for its delegate store AND aborts the cache flush started
   * in {@link SwapStores.SwapForTimeParitionedStore} - essentially an 'undo' operation.
   * <p>
   * Should be used if there is a failure during a snapshot.
   */
  public static class SwapForNonSnapshotStores extends SwapStores<IOException> {
    private static final Log LOG = LogFactory.getLog(SwapForNonSnapshotStores.class);
    private final HLog log;

    public SwapForNonSnapshotStores(HRegion parent) {
      super(parent);
      this.log = parent.log;
    }

    @Override
    public void prepare() {
      // abort the cache flush, releasing the lock
      if (log != null) {
        log.abortCacheFlush(region.getRegionInfo().getEncodedNameAsBytes());
      }

      // lock the region to swap back stores
      lockRegion();
    }

    @Override
    public void operateOnStore(Store store) throws IOException {
      if (store instanceof TimePartitionedStore) {
        // essentially unwrapping the store from the partitioning store
        // any further writes are then directed to the original store, but the partitioned store is
        // still allowed to finish its writes
        TimePartitionedStore tps = (TimePartitionedStore) store;
        LOG.debug("Replacing store:" + store + " with delegate store:" + tps.getDelgate() + ": "
            + tps.getDelgate().getClass());
        store = tps.getDelgate();
      }
      storesToSwapIn.put(store.getFamily().getName(), store);
    }
  }
}

