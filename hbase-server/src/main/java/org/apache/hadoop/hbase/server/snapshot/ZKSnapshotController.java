/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.server.snapshot;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Watches the snapshot znode, and handles the events that create snapshot
 * or abort snapshot on the region server.
 *
 * Note: there could be only one snapshot in process at any time. If a snapshot
 * has been started on this region server, only event to abort this snapshot
 * would be accepted by this class.
 */
public class ZKSnapshotController extends ZooKeeperListener implements Closeable {

  private static final String START_BARRIER_ZNODE = "start";
  private static final String END_BARRIER_ZNODE = "end";
  private static final String ABORT_ZNODE = "abort";

  protected final String startSnapshotBarrier;
  protected final String endSnapshotBarrier;
  protected final String abortZnode;

  /*
   * Layout of zk is
   * /hbase/snapshot/start/
   *                    [snapshot-name] - snapshot desc/ 
   *                        /[regions that have joined]        
   *                 /end/
   *                    [snapshot-name]/
   *                        /[regions that have finished]
   *                /abort/
   *                    [snapshot-name] - snapshot desc/
   *                        /[server that failed the snapshot]
   *  Assumption here that snapshot names are going to be unique
   */
  
  /**
   * Top-level watcher/controller for snapshots across the cluster.
   * <p>
   * On instantiation ensures the snapshot znodes exists, but requires calling
   * {@link #start()} to start monitoring for snapshots.
   * @param watcher watcher for the cluster ZK
   * @throws KeeperException when the snapshot znodes cannot be created
   */
  public ZKSnapshotController(ZooKeeperWatcher watcher) throws KeeperException {
    super(watcher);
    // make sure we are listening for events
    watcher.registerListener(this);

    // setup paths for the zknodes used in snapshotting
    String snapshotZNode = watcher.snapshotZNode;
    startSnapshotBarrier = ZKUtil.joinZNode(snapshotZNode, START_BARRIER_ZNODE);
    endSnapshotBarrier = ZKUtil.joinZNode(snapshotZNode, END_BARRIER_ZNODE);
    abortZnode = ZKUtil.joinZNode(snapshotZNode, ABORT_ZNODE);

    // first make sure all the ZK nodes exist
    ZKUtil.createAndFailSilent(watcher, startSnapshotBarrier);
    ZKUtil.createAndFailSilent(watcher, endSnapshotBarrier);
    ZKUtil.createAndFailSilent(watcher, abortZnode);
  }

  /**
   * Start monitoring nodes in ZK
   * @throws KeeperException
   */
  public void start() throws KeeperException {
    // NOOP - used by subclasses to start monitoring things they care about
  }

  @Override
  public void close() throws IOException {
    if (watcher != null) {
      watcher.close();
    }
  }

  protected static void addServerToSnapshot(ZooKeeperWatcher watcher,
      String prefix, ServerName name, Collection<HRegionInfo> regions)
      throws IOException, KeeperException {
    // build the parent znode
    String parent = ZKUtil.joinZNode(prefix, name.getServerName());
    ZKUtil.createWithParents(watcher, parent);

    // add each of the involved regions as a sub-znode
    for (HRegionInfo region : regions) {
      String node = ZKUtil.joinZNode(parent, region.getEncodedName());
      ZKUtil.createAndFailSilent(watcher, node);
    }
  }

  protected static List<String> readRegionsInSnapshot(ZooKeeperWatcher watcher,
      String serverZnode) throws KeeperException {
    return ZKUtil.listChildrenNoWatch(watcher, serverZnode);
  }
}
