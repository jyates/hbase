package org.apache.hadoop.hbase.backup;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * Monitor the actual tables for which HFiles are archived.
 * <p>
 * It is internally synchronized to ensure consistent view of the table state
 */
public class HFileArchiveTableTracker extends Configured implements HFileArchiveMonitor {
  private static final Log LOG = LogFactory.getLog(HFileArchiveTableTracker.class);
  private final Set<String> archivedTables = new TreeSet<String>();

  protected final ZooKeeperWatcher zkw;
  protected final Server parent;

  public HFileArchiveTableTracker(Server parent, ZooKeeperWatcher zkw) {
    this.parent = parent;
    this.zkw = zkw;
  }

  /**
   * Set the tables to be archived. Internally adds each table and attempts to
   * register it
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
   * Subclass hook for registering a table with external sources. Currently does
   * nothing, but can be used for things like updating ZK, etc.
   * @param table name of the table that is being archived
   */
  protected void registerTable(String table) {
    // NOOP - subclass hook
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
