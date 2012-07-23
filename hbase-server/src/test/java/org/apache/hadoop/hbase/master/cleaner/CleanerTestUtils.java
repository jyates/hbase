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
package org.apache.hadoop.hbase.master.cleaner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.regionserver.CheckedArchivingHFileCleaner;

/**
 * Utility class to help using cleaners
 */
public class CleanerTestUtils {
  private static final Log LOG = LogFactory.getLog(CleanerTestUtils.class);

  /**
   * Add the HFile cleaner checker to the list of current hfile cleaners so we can be sure the
   * cleaner checker runs.
   * @param conf configuration to update
   */
  public static void addHFileCleanerChecking(Configuration conf) {
    List<String> set = new ArrayList<String>(Arrays.asList(conf
        .getStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS)));
    set.add(0, CheckedArchivingHFileCleaner.class.getCanonicalName());
    conf.setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS, set.toArray(new String[0]));
  }

  /**
   * Make sure all the {@link HFileCleaner} run.
   * <p>
   * Blocking operation up to 3x ttl
   * <p>
   * Must be used with {@link #addHFileCleanerChecking(Configuration)}
   * @throws InterruptedException
   */
  public static void ensureHFileCleanersRun(HBaseTestingUtility util, long ttl)
      throws InterruptedException {
    CheckedArchivingHFileCleaner.resetCheck();
    do {
      util.getHBaseCluster().getMaster().getHFileCleaner().triggerNow();
      LOG.debug("Triggered, sleeping " + ttl + " ms until we can pass the check.");
      Thread.sleep(ttl);
    } while (!CheckedArchivingHFileCleaner.getChecked());
  }

}
