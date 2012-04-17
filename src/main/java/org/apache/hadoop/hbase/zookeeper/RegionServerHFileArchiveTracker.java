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
package org.apache.hadoop.hbase.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.zookeeper.KeeperException;

/**
 * RegionServer specific methods for tracking archiving of hfiles
 */
public class RegionServerHFileArchiveTracker extends HFileArchiveTracker {

  private final RegionServerServices parent;

  public RegionServerHFileArchiveTracker(ZooKeeperWatcher watcher, RegionServerServices parent) {
    super(watcher);
    this.parent = parent;
  }

  @Override
  protected HFileArchiveTableTracker createTracker() {
    // create our own special tracker that notifies the regions
    return new RegionHFileTableArchiveTracker(parent, watcher);
  }

  private static class RegionHFileTableArchiveTracker extends HFileArchiveTableTracker {

    private static final Log LOG = LogFactory.getLog(RegionHFileTableArchiveTracker.class);
    private final RegionServerServices parent;
    private final ZooKeeperWatcher watcher;

    public RegionHFileTableArchiveTracker(RegionServerServices parent, ZooKeeperWatcher watcher) {
      this.parent = parent;
      this.watcher = watcher;
    }

    /**
     * If we start archiving a table, then also post back up that we have
     * started archiving the table (two-phase commit style to ensure that
     * archiving has started).
     */
    @Override
    public synchronized void addTable(String table, String archive) {
      LOG.debug("Adding table '" + table + "' to be archived");
      if (this.tableArchiveMap.containsKey(table)) {
        LOG.debug("Already archiving table: " + table + ", ignoring it");
        return;
      }
      // first add the table to the parent
      super.addTable(table, archive);
      // notify that we are archiving the table for all regions in the RS
      try {
        String tablenode = HFileArchiveUtil.getTableNode(watcher, table);
        String rsNode = ZKUtil.joinZNode(tablenode, parent.getServerName().toString());
        ZKUtil.createEphemeralNodeAndWatch(watcher, rsNode, new byte[0]);
      } catch (KeeperException e) {
        LOG.error("Could not get online regions from parent, failing to notify that joining back of table:"
            + table);
      }
    }
  }
}
