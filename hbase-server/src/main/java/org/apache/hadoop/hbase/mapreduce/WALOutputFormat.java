package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogFactory;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Write the output of a mapreduce job to a Write-Ahead-Log that can be replayed by an HBase
 * cluster.
 */
public class WALOutputFormat extends FileOutputFormat<ImmutableBytesWritable, WALEdit> {

  private static final Log LOG = LogFactory.getLog(WALOutputFormat.class);
  /** Conf key for the M/R job to retain the source table name */
  private static String TARGET_TABLE_CONF_KEY = "hbase.waloutputformat.targettable.name";
  /**
   * Conf key to enable writing WAL output. Makes it easier to pass around as a job and use with
   * various command-line utils
   */
  public static String ENABLE_WAL_CONF_KEY = "hbase.waloutputformat.enabled";

  /**
   * target directory name of the resulting wals. "_" ensure that LoadIncrementalHFiles ignores this
   * directory when looking for HFiles
   */
  static final String WAL_OUTPUT_DIR_NAME = "_waloutput";

  public static void writeHelp() {
    System.err.println("To also write an HLog that can be used by replication");
    System.err.println("  -D" + ENABLE_WAL_CONF_KEY + "=true");
  }

  public static void updateJobIfEnabled(Job job, HTable table) throws ClassNotFoundException {
    Configuration conf = job.getConfiguration();
    // short circuit if we aren't enabled
    if (!conf.getBoolean(ENABLE_WAL_CONF_KEY, false)) {
      return;
    }
    
    //replace the reducer class with the one that also writes to the WAL
    @SuppressWarnings("rawtypes")
    Class<? extends Reducer> reducerClass = job.getReducerClass();
    if (reducerClass == KeyValueSortReducer.class) {
      job.setReducerClass(BulkLoadUtils.KeyValueWithWALSortReducer.class);
    } else if (reducerClass == PutSortReducer.class) {
      job.setReducerClass(BulkLoadUtils.PutWithWALSortReducer.class);
    } else {
      LOG.warn("Unknown reducer class for job: " + reducerClass);
    }

    // do other generic configurations we need for the output format
    conf.set(TARGET_TABLE_CONF_KEY, Bytes.toString(table.getTableName()));
    LOG.debug("Setting up WAL output from target table: " + Bytes.toString(table.getTableName()));
    // setup the multi-output
    MultipleOutputs.addNamedOutput(job, WAL_OUTPUT_DIR_NAME, WALOutputFormat.class,
      ImmutableBytesWritable.class, WALEdit.class);
  }

  @Override
  public RecordWriter<ImmutableBytesWritable, WALEdit> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = new Configuration(context.getConfiguration());

    // get info about the target table
    HBaseAdmin admin = new HBaseAdmin(conf);
    final byte[] tableName = Bytes.toBytes(conf.get(TARGET_TABLE_CONF_KEY));
    final HTableDescriptor htd = admin.getTableDescriptor(tableName);
    final List<HRegionInfo> regions =
        admin.getTableRegions(tableName);
    admin.close();

    // Get the path of the temporary output file
    final Path outputPath = FileOutputFormat.getOutputPath(context);
    final Path outputdir = new FileOutputCommitter(outputPath, context).getWorkPath();
    final FileSystem fs = outputdir.getFileSystem(conf);

    // ---------------------------------
    // custom configuration of the WAL
    // ---------------------------------

    // override logsyncher configs so it runs inline with the edits - no need to worry about delayed
    // flush
    conf.setLong("hbase.regionserver.optionallogflushinterval", 0);
    // make sure the hlog is enabled, ignoring the default configuration that may arrive
    conf.setBoolean("hbase.regionserver.hlog.enabled", true);

    // create our custom HLog
    final HLog log = HLogFactory.createHLog(fs, outputdir, WAL_OUTPUT_DIR_NAME, conf);
    LOG.debug("Creating HLog M/R writer to write to " + new Path(outputdir, WAL_OUTPUT_DIR_NAME));
    return new RecordWriter<ImmutableBytesWritable, WALEdit>() {

      @Override
      public void write(ImmutableBytesWritable rowKey, WALEdit edit) throws IOException,
          InterruptedException {
        // figure out to which hregion this edit belongs
        HRegionInfo info = null;
        for (HRegionInfo i : regions) {
          if (i.containsRow(rowKey.get())) {
            info = i;
            break;
          }
        }
        if (info == null) {
          throw new IOException("Could not find target region for row: " + rowKey);
        }
        
        // just write the edit like any other edit
        log.append(info, tableName, edit, EnvironmentEdgeManager.currentTimeMillis(), htd);
      }

      @Override
      public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
        log.close();
      }
    };
  }
}