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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.HFileArchiveMonitor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;

/**
 * Track HFile Archiving state changes in ZooKeeper.
 */
public class HFileArchiveTracker extends ZooKeeperListener implements HFileArchiveMonitor {
  private static final Log LOG = LogFactory.getLog(HFileArchiveTracker.class);
  private volatile HFileArchiveTableTracker tracker;

  public HFileArchiveTracker(ZooKeeperWatcher watcher) {
    super(watcher);
    watcher.registerListener(this);
  }

  /**
   * Start monitoring for archive updates
   * @throws KeeperException on failure to find/create nodes
   */
  public void start() throws KeeperException {
    // if archiving is enabled, then read in the list of tables to archive
    LOG.debug("Starting...");
    this.checkEnabledAndUpdate();
    LOG.debug("Finished starting!");
  }

  @Override
  public void nodeCreated(String path) {
    // if it is the archive path
    if (path.startsWith(watcher.archiveHFileZNode)) {
      LOG.debug("Archive node: " + path + " created");
      // since we are already enabled, just update a single table
      String table = path.substring(watcher.archiveHFileZNode.length());

      // if a new table has been added
      if (table.length() != 0) {
        try {
          addAndReWatchTable(path);
        } catch (KeeperException e) {
          LOG.warn("Couldn't read zookeeper data for table, not archiving", e);
        }
      }
      // the top level node has come up, so read in all the tables
      else {
        checkEnabledAndUpdate();
      }
    }
  }
  
  /**
   * Add this table to the tracker and then read a watch on that node.
   * <p>
   * Handles situtation where table is deleted in the time between the update
   * and resetting the watch by deleting the table via
   * {@link #deleteAndCheckForCreateTable(String)}
   * @param tableZnode full zookeeper path to the table to be added
   * @throws KeeperException if an unexpected zk exception occurs
   */
  private void addAndReWatchTable(String tableZnode) throws KeeperException{
    // TODO remove the data get when we move to single archive location
    tracker.addTable(ZKUtil.getNodeName(tableZnode),
      Bytes.toString(ZKUtil.getDataNoWatch(watcher, tableZnode, null)));
    //re-add a watch to the table created
    //and check to make sure it wasn't deleted
    if(!ZKUtil.watchAndCheckExists(watcher, tableZnode)){
      deleteAndCheckForCreateTable(tableZnode);
    }
  }
  
  /**
   * Delete a table from the tracker. Ensures that the table doesn't exist, in which case it adds the able back via {@link #addAndReWatchTable(String)}
   * @param tableZnode full zookeeper path to the table to be added
   * @throws KeeperException if an unexpected zk exception occurs
   */
  private void deleteAndCheckForCreateTable(String tableZnode) throws KeeperException {
    tracker.removeTable(ZKUtil.getNodeName(tableZnode));
    // if the table exists, then add and rewatch it
    if (ZKUtil.checkExists(watcher, tableZnode) >= 0) {
      addAndReWatchTable(tableZnode);
    }
  }

  @Override
  public void nodeDeleted(String path) {
    if (path.startsWith(watcher.archiveHFileZNode)) {
      LOG.debug("Archive node: " + path + " deleted");
      String table = path.substring(watcher.archiveHFileZNode.length());
      // if we stop archiving all tables
      if (table.length() == 0) {
        // make sure we have the tracker before deleting the archive
        if (tracker != null) tracker.clearArchive();
        // watches are one-time events, so we need to reup our subscription to
        // the archive node and might as well check to make sure archiving
        // didn't come back on at the same time
        checkEnabledAndUpdate();
        return;
      }
      // just stop archiving one table
      tracker.removeTable(ZKUtil.getNodeName(path));
    }
  }

  @Override
  public void nodeChildrenChanged(String path) {
    // if the archive node children changed, then we read out the new children
    if (path.startsWith(watcher.archiveHFileZNode)) {
      LOG.debug("Archive node: " + path + " children changed.");
      LOG.error("This shouldn't be called anymroe..., but not doing anything about it");

      // try {
      // updateWatchedTables();
      // } catch (KeeperException e) {
      // LOG.warn("Could not update which tables to archive", e);
      // }
    }
  }

  /**
   * Sets the watch on the top-level archive znode, and then updates the montior
   * with the current tables that should be archived (and ensures that those
   * nodes are watched as well).
   */
  private void checkEnabledAndUpdate() {
    try {
      if (ZKUtil.watchAndCheckExists(watcher, watcher.archiveHFileZNode)) {
        LOG.debug(watcher.archiveHFileZNode + " znode does exist, checking for tables to archive");

        // lazy load the tracker only if we are doing archiving
        if(tracker == null){
          synchronized(this){
            if (tracker == null) {
              this.tracker = getTracker();
            }
          }
        }

        // update the tables we should backup, to get the most recent state.
        // This is safer than also watching for children and then hoping we get
        // all the updates as it makes sure we get and watch all the children
        updateWatchedTables();
      } else LOG.debug("Archiving not currently enabling, waiting");
    } catch (KeeperException e) {
      LOG.warn("Failed to watch for archiviing znode", e);
    }
  }

  /**
   * Read the list of children under the archive znode as table names and then
   * sets those tables to the list of tables that we should archive
   * @throws KeeperException if there is an unexpected zk exception
   */
  private void updateWatchedTables() throws KeeperException {
    // get the children and watch for new children
    LOG.debug("Updating watches on tables to archive.");
    // get the children and add watches for each of the children
    List<String> tables = ZKUtil.listChildrenAndWatchThem(watcher, watcher.archiveHFileZNode);

    // if there are tables that should be archived
    if (tables != null && tables.size() > 0) {
      List<Pair<String, String>> tablesAndArchive = new ArrayList<Pair<String, String>>(
          tables.size());
      // iterate through the tables and get the archive directory we should use
      for (String table : tables) {
        String node = ZKUtil.joinZNode(watcher.archiveHFileZNode, table);
        tablesAndArchive.add(new Pair<String, String>(table, Bytes.toString(ZKUtil.getData(watcher,
          node))));
      }
      tracker.setArchiveTables(tablesAndArchive);
    } else {
      LOG.debug("No tables to archive.");
      tracker.clearArchive();
    }
  }

  @Override
  public boolean keepHFiles(String tableName) {
    return tracker.keepHFiles(tableName);
  }

  @Override
  public String getBackupDirectory(String tableName) {
    return tracker.getBackupDirectory(tableName);
  }

  /**
   * @return the tracker for which tables should be archived.
   */
  public HFileArchiveTableTracker getTracker() {
    if(this.tracker == null)
    {
      synchronized (this) {
        if(this.tracker == null)
        {
          this.tracker = this.createTracker();
        }
      }
    }
    return this.tracker;
  }

  /**
   * Set the table tracker for the overall archive tracker.
   * <p>
   * Exposed for TESTING!
   * @param tracker tracker for which tables should be archived.
   */
  public void setTracker(HFileArchiveTableTracker tracker) {
    this.tracker = tracker;
  }

  /**
   * Create a table tracker for the archive manager.
   * <p>
   * Can be overwritten by subclasses to provide special tracking behavior
   * @return tracker for which tables should be archived.
   */
  protected HFileArchiveTableTracker createTracker() {
    return new HFileArchiveTableTracker();
  }

  /**
   * Management of the actual tables to be archived, etc
   * <p>
   * It is internally synchronized to ensure consistent view of the table state
   */
  public static class HFileArchiveTableTracker implements HFileArchiveMonitor {
    private final Map<String, String> tableArchiveMap = new HashMap<String, String>();

    // TODO move this from a map to just a list and a single lookup for the
    // config value - archive directory is set per-cluster
    public synchronized void setArchiveTables(List<Pair<String, String>> tablesAndArchives) {
      tableArchiveMap.clear();
      for (Pair<String, String> ta : tablesAndArchives) {
        addTable(ta.getFirst(), ta.getSecond());
      }
    }

    public synchronized void addTable(String table, String archive) {
      tableArchiveMap.put(table, archive);
    }

    public synchronized void removeTable(String table) {
      tableArchiveMap.remove(table);
    }

    public synchronized void clearArchive() {
      tableArchiveMap.clear();
    }

    @Override
    public synchronized boolean keepHFiles(String tableName) {
      return tableArchiveMap.containsKey(tableName);
    }

    @Override
    public synchronized String getBackupDirectory(String tableName) {
      return this.tableArchiveMap.get(tableName);
    }
  }
}
