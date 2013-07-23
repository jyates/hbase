package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.StringUtils;

/**
 * Utility class for dealing with bulk-loading HFiles
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class BulkLoadUtils {

  private static class WALWritingReducerReducer<K, V> extends
      Reducer<K, V, ImmutableBytesWritable, KeyValue> {
    @SuppressWarnings("rawtypes")
    protected MultipleOutputs mos;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected void setup(Context context) {
      mos = new MultipleOutputs(context);
    }

    protected void write(ImmutableBytesWritable row, Set<KeyValue> map, Context context)
        throws IOException, InterruptedException {
      int index = 0;
      WALEdit edit = new WALEdit(false);
      for (KeyValue kv : map) {
        context.write(row, kv);
        if (index > 0 && index % 100 == 0) context.setStatus("Wrote " + index);
        // build up the WALEDit as well
        edit.add(kv);
      }
      // write to the corresponding WAL file
      mos.write(WALOutputFormat.WAL_OUTPUT_DIR_NAME, row, edit);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      mos.close();
    }

  }

  /**
   * Similar to the {@link KeyValueSortReducer}, except we also write to a matching HFile
   * @see HFileOutputFormat
   */
  public static class KeyValueWithWALSortReducer extends
      WALWritingReducerReducer<ImmutableBytesWritable, KeyValue> {

    protected void reduce(ImmutableBytesWritable row, java.lang.Iterable<KeyValue> kvs,
        Context context) throws java.io.IOException, InterruptedException {
      // new map for each row, so we are just sorting a single row
      TreeSet<KeyValue> map = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
      for (KeyValue kv : kvs) {
        try {
          map.add(kv.clone());
        } catch (CloneNotSupportedException e) {
          throw new java.io.IOException(e);
        }
      }
      context.setStatus("Read " + map.getClass());
      write(row, map, context);
    }

    public static void addToJob(Job job) {
      MultipleOutputs.addNamedOutput(job, WALOutputFormat.WAL_OUTPUT_DIR_NAME, WALOutputFormat.class,
        ImmutableBytesWritable.class, WALEdit.class);
    }
  }

  public static class PutWithWALSortReducer extends
      WALWritingReducerReducer<ImmutableBytesWritable, Put> {

    @Override
    protected void reduce(ImmutableBytesWritable row, java.lang.Iterable<Put> puts,
        Reducer<ImmutableBytesWritable, Put, ImmutableBytesWritable, KeyValue>.Context context)
        throws java.io.IOException, InterruptedException {
      // although reduce() is called per-row, handle pathological case
      long threshold =
          context.getConfiguration().getLong("putsortreducer.row.threshold", 2L * (1 << 30));
      Iterator<Put> iter = puts.iterator();
      while (iter.hasNext()) {
        TreeSet<KeyValue> map = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
        long curSize = 0;
        // stop at the end or the RAM threshold
        while (iter.hasNext() && curSize < threshold) {
          Put p = iter.next();
          for (List<? extends Cell> cells : p.getFamilyMap().values()) {
            for (Cell cell : cells) {
              KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
              map.add(kv);
              curSize += kv.getLength();
            }
          }
        }
        context.setStatus("Read " + map.size() + " entries of " + map.getClass() + "("
            + StringUtils.humanReadableInt(curSize) + ")");
        write(row, map, context);

        // if we have more entries to process
        if (iter.hasNext()) {
          // force flush because we cannot guarantee intra-row sorted order
          context.write(null, null);
        }
      }
    }
  }
}
