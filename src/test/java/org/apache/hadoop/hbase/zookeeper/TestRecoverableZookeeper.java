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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TestZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(MediumTests.class)
public class TestRecoverableZookeeper {

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @Test
  public void testRecursiveDelete() throws Exception {
    UTIL.startMiniZKCluster();
    try {
      final ZooKeeperWatcher zkw = new ZooKeeperWatcher(new Configuration(UTIL.getConfiguration()),
          TestZooKeeper.class.getName(), null);
      RecoverableZooKeeper rzk = zkw.getRecoverableZooKeeper();

      // set the stored zk as a spy of the real one
      ZooKeeper real = rzk.zk;
      ZooKeeper zkSpy = Mockito.spy(real);
      rzk.zk = zkSpy;

      // nodes to put into zk
      final String child = "/root/parent/child";
      final String parent = ZKUtil.getParent(child);
      final String sibling = ZKUtil.joinZNode(parent, "sibling");
      final String root = ZKUtil.getParent(parent);

      // add a node to zk
      ZKUtil.createWithParents(zkw, child);
      ZKUtil.createWithParents(zkw, sibling);
      assertTrue(ZKUtil.checkExists(zkw, child) >= 0);

      // and then recursively delete a parent;
      rzk.deleteRecursively(root, -1);

      // expect the call to get the first child
      Mockito.verify(zkSpy).getChildren(root, false);
      // and then that has a child, and we also get the sub-children
      Mockito.verify(zkSpy).getChildren(parent, false);
      Mockito.verify(zkSpy).getChildren(child, false);
      Mockito.verify(zkSpy).getChildren(sibling, false);

      // after which everything should be deleted
      assertEquals(-1, ZKUtil.checkExists(zkw, root));

      // and then check to make sure we don't throw an exception if the node
      // doesn't exist
      rzk.deleteRecursively(root, -1);

      // make sure we do fail if we have the wrong version
      ZKUtil.createAndFailSilent(zkw, root);
      // verify that we call attempt the bad transaction once
      try {
        rzk.deleteRecursively(root, 2);
        fail("Delete recursively didn't throw an error for wrong version");
      } catch (KeeperException e) {
        // NOOP - it works correctly
      }
      // ensure the total number of transactions created for this test
      Mockito.verify(zkSpy, Mockito.times(3)).transaction();

    } finally {
      UTIL.getZkCluster().shutdown();
    }
  }
}
