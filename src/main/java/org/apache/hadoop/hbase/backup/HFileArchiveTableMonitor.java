package org.apache.hadoop.hbase.backup;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Monitor the actual tables for which HFiles are archived.
 * <p>
 * It is internally synchronized to ensure consistent view of the table state
 */
public class HFileArchiveTableMonitor extends Configured implements HFileArchiveMonitor {
  private static final Log LOG = LogFactory.getLog(HFileArchiveTableMonitor.class);
  private final Set<String> archivedTables = new TreeSet<String>();

  protected final ZooKeeperWatcher zkw;
  protected final Server parent;

  /**
   * Exposed for testing only. Generally, the
   * {@link TableHFileArchiveTracker#create(ZooKeeperWatcher, Server)} should be used
   * when working with table archive monitors.
   * @param parent server for which we should monitor archiving
   * @param zkw watcher for the zookeeper cluster for archiving hfiles
   */
  HFileArchiveTableMonitor(Server parent, ZooKeeperWatcher zkw) {
    this.parent = parent;
    this.zkw = zkw;
  }

  /**
   * Set the tables to be archived. Internally adds each table and attempts to
   * register it.
   * <p>
   * <b>Note: All previous tables will be removed in favor of these tables.<b>
   * @param tables add each of the tables to be archived.
   */
  public synchronized void setArchiveTables(List<String> tables) {
    archivedTables.clear();
    archivedTables.addAll(tables);
    for (String table : tables) {
      registerTable(table);
    }
  }

  /**
   * Add the named table to be those being archived. Attempts to register the
   * table
   * @param table name of the table to be registered
   */
  public synchronized void addTable(String table) {
    if (this.keepHFiles(table)) {
      LOG.debug("Already archiving table: " + table + ", ignoring it");
      return;
    }
    archivedTables.add(table);
    registerTable(table);
  }

  /**
   * Subclass hook for registering a table with external sources.
   * <p>
   * Currently updates ZK with the fact that the current server has started
   * archiving hfiles for the specified table.
   * @param table name of the table that is being archived
   */
  protected void registerTable(String table) {
    LOG.debug("Adding table '" + table + "' to be archived.");

    // notify that we are archiving the table for this server
    try {
      String tablenode = HFileArchiveUtil.getTableNode(zkw, table);
      String serverNode = ZKUtil.joinZNode(tablenode, parent.getServerName().toString());
      ZKUtil.createEphemeralNodeAndWatch(zkw, serverNode, new byte[0]);
    } catch (KeeperException e) {
      LOG.error("Failing to update that this server(" + parent.getServerName()
          + " is joining archive of table:" + table);
    }
  }

  public synchronized void removeTable(String table) {
    archivedTables.remove(table);
  }

  public synchronized void clearArchive() {
    archivedTables.clear();
  }

  @Override
  public synchronized boolean keepHFiles(String tableName) {
    return archivedTables.contains(tableName);
  }
}
