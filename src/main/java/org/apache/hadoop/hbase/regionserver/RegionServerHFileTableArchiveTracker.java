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
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.backup.HFileArchiveTableTracker;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Tracker that also updates ZK that a region has received the update start
 * archiving a table.
 */
class RegionServerHFileTableArchiveTracker extends HFileArchiveTableTracker {

  private static final Log LOG = LogFactory.getLog(RegionServerHFileTableArchiveTracker.class);

  public RegionServerHFileTableArchiveTracker(Server parent, ZooKeeperWatcher watcher) {
    super(parent, watcher);
  }

  /**
   * If we start archiving a table, then also post back up that we have started
   * archiving the table (two-phase commit style to ensure that archiving has
   * started).
   */
  @Override
  public synchronized void registerTable(String table) {
    LOG.debug("Adding table '" + table + "' to be archived");

    // notify that we are archiving the table for all regions in the RS
    try {
      String tablenode = HFileArchiveUtil.getTableNode(zkw, table);
      String rsNode = ZKUtil.joinZNode(tablenode, parent.getServerName().toString());
      ZKUtil.createEphemeralNodeAndWatch(zkw, rsNode, new byte[0]);
    } catch (KeeperException e) {
      LOG.error("Could not get online regions from parent, failing to notify that joining archive of table:"
          + table);
    }
  }
}
