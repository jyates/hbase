package org.apache.hadoop.hbase.backup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileArchiveTestingUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test that the {@link HFileDisposer} correctly removes all the parts of a
 * region when cleaning up a region
 */
@Category(MediumTests.class)
public class TestRegionDisposer {

  private static final String STRING_TABLE_NAME = "test_table";

  private static final Log LOG = LogFactory.getLog(TestRegionDisposer.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte[] TABLE_NAME = Bytes.toBytes(STRING_TABLE_NAME);
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");

  /**
   * Setup the config for the cluster
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
  }

  private static void setupConf(Configuration conf) {
    // disable the ui
    conf.setInt("hbase.regionsever.info.port", -1);
    // drop the memstore size so we get flushes
    conf.setInt("hbase.hregion.memstore.flush.size", 25000);
  }

  @Before
  public void startMinicluster() throws Exception {
    UTIL.startMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    // cleanup the cluster if its up still
    if (UTIL.getHBaseAdmin().tableExists(STRING_TABLE_NAME)) {

      UTIL.deleteTable(TABLE_NAME);
    }
    // and cleanup the archive directory
    try {
      UTIL.getTestFileSystem().delete(new Path(UTIL.getDefaultRootDirPath(), ".archive"), true);
    } catch (IOException e) {
      LOG.warn("Failure to delete archive directory", e);
    }
    // make sure that backups are off for all tables
    UTIL.getHBaseAdmin().disableHFileBackup();
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      // NOOP;
    }
  }

  @Test
  public void testRemovesRegionDirOnArchive() throws Exception {
    UTIL.createTable(TABLE_NAME, TEST_FAM);
    final HBaseAdmin admin = UTIL.getHBaseAdmin();

    // get the current store files for the region
    List<HRegion> servingRegions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    // make sure we only have 1 region serving this table
    assertEquals(1, servingRegions.size());
    HRegion region = servingRegions.get(0);

    // turn on archiving
    admin.enableHFileBackup(TABLE_NAME);

    // and load the table
    UTIL.loadRegion(region, TEST_FAM);

    // shutdown the table so we can manipulate the files
    admin.disableTable(STRING_TABLE_NAME);

    FileSystem fs = UTIL.getTestFileSystem();

    // now attempt to depose the region
    Path regionDir = HRegion.getRegionDir(region.getTableDir().getParent(), region.getRegionInfo());

    HFileArchiveMonitor monitor = Mockito.mock(HFileArchiveMonitor.class);
    Mockito.when(monitor.keepHFiles(STRING_TABLE_NAME)).thenReturn(true);
    HFileDisposer.disposeRegion(fs, monitor, region.getRegionInfo());
    
    // check for the existence of the archive directory and some files in it
    Path archiveDir = HFileArchiveTestingUtil.getRegionArchiveDir(UTIL.getConfiguration(), region);
    assertTrue(fs.exists(archiveDir));

    // check to make sure the store directory was copied
    FileStatus[] stores = fs.listStatus(archiveDir);
    assertTrue(stores.length == 1);

    // make sure we archived the store files
    FileStatus[] storeFiles = fs.listStatus(stores[0].getPath());
    assertTrue(storeFiles.length > 0);

    // then ensure the region's directory isn't present
    assertFalse(fs.exists(regionDir));

    //recreate the table
    admin.deleteTable(STRING_TABLE_NAME);
    UTIL.createTable(TABLE_NAME, TEST_FAM);
    
    // now copy back in the region
    // fs.copyFromLocalFile(archive, regionDir);

    // and depose the region without archiving
    Mockito.when(monitor.keepHFiles(STRING_TABLE_NAME)).thenReturn(false);
    HFileDisposer.disposeRegion(fs, monitor, region.getRegionInfo());

    assertFalse(fs.exists(regionDir));
  }
}
