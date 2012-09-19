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

/**
 * General operation to apply each of the hstores.
 */
public abstract class StoreOperator<E extends Exception> {

  protected HRegion region;

  /**
   * Create a store operation on the given region
   * @param parent region on which to operate
   */
  public StoreOperator(HRegion parent) {
    this.region = parent;
  }

  /**
   * Run once in preparation to operate on stores
   */
  public abstract void prepare();

  /**
   * Run multiple times, once on each store
   * @param store the store to operate on
   */
  public abstract void operateOnStore(Store store) throws E;

  /**
   * Run after each of the stores has been operated on or an exception is thrown.
   * <p>
   * Guaranteed to run.
   */
  public abstract void finish();
}