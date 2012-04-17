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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.util.HFileArchiveUtil.getTableNode;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Client-side manager for which tables to archive
 */
public class HFileArchiveManager {

  private static final Log LOG = LogFactory.getLog(HFileArchiveManager.class);
  private final ZooKeeperWatcher zooKeeper;

  public HFileArchiveManager(HConnection connection, Configuration conf)
      throws ZooKeeperConnectionException, IOException {
    this(new ZooKeeperWatcher(conf, "hfileArchiveManger-on-"
        + connection.toString(), connection));
  }

  public HFileArchiveManager(ZooKeeperWatcher watcher) {
    this.zooKeeper = watcher;
  }

  /**
   * Turn on auto-backups of HFiles on the specified table.
   * <p>
   * When HFiles would be deleted, they are instead interned to the backup
   * directory specified or HConstants#DEFAULT_HFILE_ARCHIVE_DIRECTORY
   * @param table
   * @param archive
   * @throws IOException
   */
  public void enableHFileBackup(byte[] table, byte[] archive)
      throws IOException {
    try {
      enable(this.zooKeeper, table, archive);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * Table backups on HFiles for the given table
   * @param table
   * @throws IOException
   */
  public void disableHFileBackup(byte[] table) throws IOException {
    try {
      disable(this.zooKeeper, table);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * Disable backups on all tables in the cluster
   * @throws IOException if the number of attempts is exceeded
   */
  public void disableHFileBackup() throws IOException {
    LOG.debug("Disabling backups on all tables.");
    try {
      ZKUtil.deleteNodeRecursively(this.zooKeeper, this.zooKeeper.archiveHFileZNode);
      return;
    } catch (KeeperException e) {
      throw new IOException("Unexpected ZK exception!", e);
    }
  }

  /**
   * Get the current list of all the regionservers that are currently involved
   * in archiving the table
   * @param table name of table under which to check for regions that are
   *          archiving.
   * @return the currently online regions that are archiving the table
   * @throws IOException if an unexpected connection issues occurs
   */
  @SuppressWarnings("unchecked")
  public List<String> regionServersArchiving(byte[] table) throws IOException {
    try {
      // build the table znode
      String tableNode = getTableNode(zooKeeper, table);
      List<String> regions = ZKUtil.listChildrenNoWatch(zooKeeper, tableNode);
      return (List<String>) (regions == null ? Collections.emptyList() : regions);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * Best effort enable of table backups. If a region serving a table is
   * offline, it will be notified on startup.
   * <p>
   * No attempt is made to make sure that backups are successfully created - it
   * is inherently an <b>asynchronous operation</b>.
   * @param zooKeeper watcher connection to zk cluster
   * @param table table name on which to enable archiving
   * @param archive name of the directory under the table (in hdfs) to archive
   *          files
   * @throws KeeperException
   */
  private void enable(ZooKeeperWatcher zooKeeper, byte[] table, byte[] archive)
      throws KeeperException {
    // make sure the archive znode exists
    LOG.debug("Ensuring archiving znode exists");
    ZKUtil.createAndFailSilent(zooKeeper, zooKeeper.archiveHFileZNode);

    // then add the table to the list of znodes to archive
    String tableNode = getTableNode(zooKeeper, table);
    LOG.debug("Creating: " + tableNode + ", data:" + Bytes.toString(archive));
    ZKUtil.createSetData(zooKeeper, tableNode, archive);
  }

  /**
   * Disable all archiving of files for a given table
   * <p>
   * <b>Note: Asynchronous</b>
   * @param zooKeeper watcher for the ZK cluster
   * @param table name of the table to disable
   * @throws KeeperException if an unexpected ZK connection issues occurs
   */
  private void disable(ZooKeeperWatcher zooKeeper, byte[] table)
      throws KeeperException {
    // ensure the latest state of the archive node is found
    zooKeeper.sync(zooKeeper.archiveHFileZNode);
    // make sure that we have enabled archiving in the first place
    if (ZKUtil.checkExists(zooKeeper, zooKeeper.archiveHFileZNode) < 0) {
      return;
    }

    // delete the table node, from the archive - will be noticed by
    // regionservers
    String tableNode = getTableNode(zooKeeper, table);
    // make sure the table is the latest version so the delete takes
    zooKeeper.sync(tableNode);
    // LOG.debug("Attempting to delete:" + tableNode);
    ZKUtil.deleteNodeRecursively(zooKeeper, tableNode);
  }
}
