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
package org.apache.hadoop.hbase.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.mortbay.log.Log;

/**
 * Similar to {@link LoadIncrementalHFiles}, load the output WALs into the cluster for replication.
 */
public class BulkLoadWALs {

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    System.exit(new BulkLoadWALs(conf).run(args));
  }

  private void usage() {
    System.err.println("usage: " + this.getClass().getName() + " /path/to/job-output");
  }
  
  private final Configuration conf;
  
  public BulkLoadWALs(Configuration conf) {
    this.conf = conf;
  }
  
  /**
   * Run the bulk load of the WALs for replication.
   * @param conf A working configuration to connect to the cluster.
   * @param args
   * @return 0 for success
   */
  public int run(String[] args) throws IOException {
    if (args.length > 0) {
      usage();
      throw new IllegalArgumentException("No path to target directory present!");
    }
    String path = args[0];
    try {
      Path[] wals = getWals(new Path(path));
      if (wals.length == 0) {
        Log.info("No WALs found in " + path + ", done!");
        return 0;
      }

      // figure out the regionserver to which we should distribute the logs
      HBaseAdmin admin = new HBaseAdmin(conf);
      Collection<ServerName> servers = admin.getClusterStatus().getServers();
      admin.close();

      // get the distribution of WAL files
      int logsPerServer = servers.size() / wals.length;
      if(logsPerServer == 0) {
        logsPerServer =1;
      }

      // TODO write down the WALs we need to replay so we can roll forward during failure

      // add the logs evenly over the servers
      int walIndex = 0;
      while (walIndex < wals.length) {
        for (ServerName server : servers) {
          // add the right number of logs to the server
          for (int c = 0; c < logsPerServer; c++) {
            addLogToServer(wals[walIndex++], server);
          }
        }
      }
      
      return 0;
    }catch(IOException e) {
      System.err.println("Failed to Load WALs: "+e.getMessage());
      e.printStackTrace(System.err);
      usage();
      throw e;
    }
  }



  private Path[] getWals(Path path) throws IOException {
    //find the WALs on the path
    FileSystem fs = path.getFileSystem(conf);
    Path walDir = new Path(path, WALOutputFormat.WAL_OUTPUT_DIR_NAME);
    if(!fs.exists(walDir)) {
      throw new FileNotFoundException("WALOutputFormat directory "+path+ "not found");
    }
    
    FileStatus [] wals = fs.listStatus(walDir);
    if(wals == null) {
      throw new FileNotFoundException("No WAL files found in "+path);
    }
    return FileUtil.stat2Paths(wals);
  }


  private void setupReplicationConnection() {
    // get all the per clusters for which we need to create queues
    for (String id : this.zkHelper.getPeerClusters()/* the znode for the peer cluster id */) {
      addSource(id);
    }
  }
  
  
  /**
   * @param path
   * @param server
   */
  private void addLogToServer(Path path, ServerName server) {
    
  }

}
