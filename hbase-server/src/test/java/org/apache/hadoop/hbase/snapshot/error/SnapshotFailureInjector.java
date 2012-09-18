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
package org.apache.hadoop.hbase.snapshot.error;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.errorhandling.impl.CheckableFaultInjector;
import org.apache.hadoop.hbase.server.errorhandling.impl.FaultInjectionPolicy;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Simple failure injector to use for propagating a failure to a running snapshot
 */
public class SnapshotFailureInjector extends CheckableFaultInjector<HBaseSnapshotException> {

  private final HBaseTestingUtility util;

  /**
   * @param policy to apply when checking for failure
   * @param util utility running the current test cluster
   */
  public SnapshotFailureInjector(FaultInjectionPolicy policy, HBaseTestingUtility util) {
    super(policy);
    this.util = util;
  }

  @Override
  protected Pair<HBaseSnapshotException, Object[]> getInjectedError(StackTraceElement[] trace) {
    // mark that we got the error request
    super.getInjectedError(trace);
    // build our own response
    SnapshotDescription runningSnapshot = util.getHBaseCluster().getMaster().getSnapshotManager()
        .getCurrentSnapshotMonitor().getSnapshot();
    return new Pair<HBaseSnapshotException, Object[]>(new HBaseSnapshotException("injected"),
        new Object[] { runningSnapshot });
  }
}