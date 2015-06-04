package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test that an HBase cluster can run on top of an existing MiniDfsCluster
 */
@Category(MediumTests.class)
public class TestHBaseOnOtherDfsCluster {

  @Test
  public void testOveralyOnOtherCluster() throws Exception {
    // just run HDFS
    HBaseTestingUtility util1 = new HBaseTestingUtility();
    MiniDFSCluster dfs = util1.startMiniDFSCluster(1);

    // run HBase on that HDFS
    HBaseTestingUtility util2 = new HBaseTestingUtility();
    // set the dfs
    util2.setDFSCluster(dfs, false);
    util2.startMiniCluster();

    //ensure that they are pointed at the same place
    FileSystem fs = dfs.getFileSystem();
    FileSystem targetFs = util2.getDFSCluster().getFileSystem();
    assertFsSameUri(fs, targetFs);

    fs = FileSystem.get(util1.getConfiguration());
    targetFs = FileSystem.get(util2.getConfiguration());
    assertFsSameUri(fs, targetFs);

    Path randomFile = new Path("/"+UUID.randomUUID());
    assertTrue(targetFs.createNewFile(randomFile));
    assertTrue(fs.exists(randomFile));

    // do a simple create/write to ensure the cluster works as expected
    byte[] family = Bytes.toBytes("testfamily");
    byte[] tablename = Bytes.toBytes("testtable");
    HTable table = util2.createTable(tablename, family);
    Put p = new Put(new byte[] { 1, 2, 3 });
    p.add(family, null, new byte[] { 1 });
    table.put(p);
    table.flushCommits();

    // shutdown and make sure cleanly shutting down
    util2.shutdownMiniCluster();
    util1.shutdownMiniDFSCluster();
  }

  private void assertFsSameUri(FileSystem sourceFs, FileSystem targetFs) {
    Path source = new Path(sourceFs.getUri());
    Path target = new Path(targetFs.getUri());
    assertEquals(source, target);
  }
}