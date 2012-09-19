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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

abstract class LockRegionOperator<E extends Exception> extends StoreOperator<E> {
  private static final Log LOG = LogFactory.getLog(LockRegionOperator.class);

  public LockRegionOperator(HRegion parent) {
    super(parent);
  }

  @Override
  public void prepare() {
    // take a write lock on the region
    lockRegion();
  }

  /**
   * Lock the region for writes. Make sure to call {@link #unlockRegion()} after the application has
   * completed
   */
  protected final void lockRegion() {
    LOG.debug("Locking region before store operation.");
    region.updatesLock.writeLock().lock();
  }

  @Override
  public void finish() {
    // unlock the region for writes
    unlockRegion();
  }

  /**
   * Unlock the region, allowing writes. Must be called after {@link #lockRegion()}
   */
  protected final void unlockRegion() {
    LOG.debug("Unlocking region after store operation.");
    region.updatesLock.writeLock().unlock();
  }
}