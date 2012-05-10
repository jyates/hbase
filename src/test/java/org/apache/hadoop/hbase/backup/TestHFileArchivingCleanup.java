package org.apache.hadoop.hbase.backup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.backup.HFileArchiveCleanup;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileArchiveTestingUtil;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestHFileArchivingCleanup {

  private static final Log LOG = LogFactory.getLog(TestHFileArchivingCleanup.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final String STRING_TABLE_NAME = "test";
  private static final byte[] TABLE_NAME = Bytes.toBytes(STRING_TABLE_NAME);
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");

  /**
   * Setup the config for the cluster
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster();
  }

  private static void setupConf(Configuration conf) {
    // disable the ui
    conf.setInt("hbase.regionsever.info.port", -1);
  }

  @Before
  public void setup() throws Exception {
    UTIL.createTable(TABLE_NAME, TEST_FAM);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.deleteTable(TABLE_NAME);
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

  /**
   * Test that when an attempt to start archiving fails we don't have unknown
   * archive files lying around
   * @throws Exception
   */
  @Test
  public void testMasterDeletesFailedArchive() throws Exception {
    final HBaseAdmin admin = UTIL.getHBaseAdmin();

    // get the current store files for the region
    List<HRegion> servingRegions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    // make sure we only have 1 region serving this table
    assertEquals(1, servingRegions.size());
    final HRegion region = servingRegions.get(0);

    // turn on archiving
    admin.enableHFileBackup(TABLE_NAME);
    LOG.debug("----Starting test of cleanup");
    // so lets put some files in the archive that are newer than the start
    FileSystem fs = UTIL.getTestFileSystem();
    Path archiveDir = HFileArchiveTestingUtil.getTableArchivePath(UTIL.getConfiguration(), region);
    // write a tmp file to the archive dir
    Path tmpFile = new Path(archiveDir, "toDelete");
    FSDataOutputStream out = fs.create(tmpFile);
    out.write(1);
    out.close();
    LOG.debug("Created toDelete");

    // now run the cleanup util
    HFileArchiveCleanup.setConfiguration(UTIL.getConfiguration());
    HFileArchiveCleanup.main(new String[0]);
    // make sure the fake archived file has been cleaned up
    assertFalse(fs.exists(tmpFile));

    // now do the same create again, but make sure it still exists since we have
    // an earlier end time
    long end = EnvironmentEdgeManager.currentTimeMillis();

    LOG.debug("re-created toDelete");
    // write a tmp file to the archive dir
    out = fs.create(tmpFile);
    out.write(1);
    out.close();
    HFileArchiveCleanup.main(new String[] { "-e", Long.toString(end) });
    assertTrue(fs.exists(tmpFile));

    LOG.debug("Still not deleting the file");
    // now bump the start time to match the end time - still should be there
    HFileArchiveCleanup.main(new String[] { "-s", Long.toString(end), "-e", Long.toString(end) });
    assertTrue(fs.exists(tmpFile));

    // now move the start up to include the file, which should delete it
    LOG.debug("Now should delete the file");
    HFileArchiveCleanup.main(new String[] { "-s",
        Long.toString(fs.getFileStatus(tmpFile).getModificationTime()) });
    assertFalse(fs.exists(tmpFile));

    // now create the files in multiple table directories, and check that we
    // only delete the one in the specified directory
    out = fs.create(tmpFile);
    out.write(1);
    out.close();

    // create the table archive and put a file in there
    Path tableArchive = new Path(archiveDir, STRING_TABLE_NAME);
    fs.mkdirs(tableArchive);
    Path tableFile = new Path(tableArchive, "table");
    out = fs.create(tableFile);
    out.write(1);
    out.close();

    // now delete just that table
    LOG.debug("Just cleaning up table: table, files:" + tableFile);
    HFileArchiveCleanup
        .main(new String[] { "-s",
            Long.toString(fs.getFileStatus(tableFile).getModificationTime()), "-t",
            STRING_TABLE_NAME });
    assertTrue(fs.exists(tmpFile));
    assertFalse(fs.exists(tableFile));
  }
}
