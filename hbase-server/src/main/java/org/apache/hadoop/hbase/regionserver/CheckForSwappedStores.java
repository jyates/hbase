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

/**
 * Simple check to see if any of the current stores are swapped.
 */
public class CheckForSwappedStores extends StoreOperator<IOException> {
  public boolean foundSwappedStores = false;

  public CheckForSwappedStores(HRegion parent) {
    super(parent);
  }

  @Override
  public void prepare() {
    // NOOP
  }

  @Override
  public void operateOnStore(Store store) throws IOException {
    // if we didn't find a swapped store, then check if it swapped.
    if (!foundSwappedStores && store instanceof TimePartitionedStore) foundSwappedStores = true;
  }

  @Override
  public void finish() {
    // NOOP
  }
}