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
package org.apache.hadoop.hbase.backup;

import java.io.IOException;
import java.util.Stack;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;

/**
 * Utility to help cleanup extra archive files. This is particularly useful if
 * there is a partially completed archive, but one of the regionservers bailed
 * before it could be completed.
 * <p>
 * Run with -h to see help.
 */
public class HFileArchiveCleanup {

  private static final Log LOG = LogFactory.getLog(HFileArchiveCleanup.class);

  private static Configuration conf;

  // configuration
  private long start = -1;
  private long end = Long.MAX_VALUE;
  private String table = null;
  private boolean all = true;

  public static void main(String[] args) throws Exception {
    // setup
    HFileArchiveCleanup cleaner = new HFileArchiveCleanup();
    try {
      if (!cleaner.parse(args)) {
        System.exit(-1);
      }
    } catch (ParseException e) {
      LOG.error("Parse Exception: " + e.getMessage());
      cleaner.printHelp();
      System.exit(-1);
    }

    // run the cleanup
    Configuration conf = getConf();
    FileSystem fs = FSUtils.getCurrentFileSystem(conf);

    Path archiveDir;
    // if we aren't cleaning a specific table, then get the table directory
    if (!cleaner.all) {
      Path tableDir = HTableDescriptor.getTableDir(FSUtils.getRootDir(conf),
        Bytes.toBytes(cleaner.table));
      archiveDir = HFileArchiveUtil.getTableArchivePath(conf, tableDir);
    } else {
      archiveDir = new Path(FSUtils.getRootDir(conf),
          HFileArchiveUtil.getConfiguredArchiveDir(conf));
    }

    LOG.debug("Cleaning up: " + archiveDir);
    // iterate through the archive directory and delete everything that falls
    // outside the range
    Stack<Path> directories = new Stack<Path>();
    directories.add(archiveDir);
    // while there are more directories to look at
    while (!directories.isEmpty()) {
      Path parent = directories.pop();
      // if the parent exists
      if (fs.exists(parent)) {
        FileStatus[] children = fs.listStatus(parent);
        for (FileStatus child : children) {
          // push directory down to be cleaned
          if (child.isDir()) {
            directories.push(child.getPath());
            continue;
          }
          // otherwise its a file and we can check deletion
          // we have to check mod times against what the command line says
          long mod = child.getModificationTime();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Child file: " + child.getPath() + ", has mod time:" + mod);
          }
          // check to see if it meets the credentials
          if (cleaner.start <= mod && mod < cleaner.end) {
            try {
              LOG.debug("Start:" + cleaner.start + " =< " + mod + " < " + cleaner.end + ": end");
              fs.delete(child.getPath(), false);
            } catch (IOException e) {
              LOG.warn("Failed to delete child", e);
            }
          }
        }
      }
    }
  }

  /**
   * Set the configuration to use.
   * <p>
   * Exposed for TESTING!
   * @param conf
   */
  static void setConfiguration(Configuration conf) {
    HFileArchiveCleanup.conf = conf;
  }

  private synchronized static Configuration getConf() {
    if (HFileArchiveCleanup.conf == null) {
      setConfiguration(HBaseConfiguration.create());
    }
    return HFileArchiveCleanup.conf;
  }

  // ---------------------------
  // Setup and options parsing
  // ---------------------------

  private final Options opts;
  private final Parser parser;

  @SuppressWarnings("static-access")
  public HFileArchiveCleanup() {
    // create all the necessary options
    opts = new Options();
    // create the time option
    OptionBuilder
        .hasArg()
        .withArgName("start time")
        .withDescription(
          "Start time from which to start removing"
              + " backup files in seconds from the epoch (inclusive). "
              + "If not set, all archive files are removed from the "
              + "specified table up to the end time. Not setting start "
              + "OR end time deletes all archived files.");
    opts.addOption(OptionBuilder.create('s'));

    // end time option
    OptionBuilder
        .hasArg()
        .withArgName("end time")
        .withDescription(
          "End time from which to start removing backup files in seconds "
              + "from the epoch (exclusive). If not set, all archive files forward "
              + "from the start time are removed. Not setting start OR end time deletes "
              + "all archived files.");
    opts.addOption(OptionBuilder.create('e'));

    // create the tablename option
    OptionBuilder
        .hasArg()
        .withArgName("table")
        .withDescription(
          "Name of the table for which to remove backups. If not set, all archived "
              + "files will be cleaned up.")
        .withType(String.class);
    opts.addOption(OptionBuilder.create('t'));

    // add the help
    opts.addOption(OptionBuilder.hasArg(false).withDescription("Show this help").create('h'));

    // create the parser
    parser = new GnuParser();
  }

  public boolean parse(String[] args) throws ParseException {
    CommandLine cmd;
    cmd = parser.parse(opts, args);

    if (cmd.hasOption('h')) {
      printHelp();
      return false;
    }
    // get start/end times
    if (cmd.hasOption('s')) {
      this.start = Long.parseLong(cmd.getOptionValue('s'));
      LOG.debug("Setting start time to:" + start);
    }
    if (cmd.hasOption('e')) {
      this.end = Long.parseLong(cmd.getOptionValue('e'));
      LOG.debug("Setting end time to:" + end);
    }

    if (start > end) {
      throw new ParseException("Start time cannot exceed end time.");
    }

    // get the table to cleanup
    if (cmd.hasOption('t')) {
      this.table = cmd.getOptionValue('t');
      this.all = false;
    }
    return true;
  }

  public void printHelp() {
    HelpFormatter help = new HelpFormatter();
    help.printHelp("java HFileArchiveCleanup", opts);
  }
}
