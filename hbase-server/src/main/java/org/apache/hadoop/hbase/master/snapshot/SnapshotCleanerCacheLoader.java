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
package org.apache.hadoop.hbase.master.snapshot;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

/**
 * Delegate that does the cache loading for a snapshot file cleaner.
 * @see SnapshotLogCleaner
 * @see SnapshotHFileCleaner
 * @see SnapshotCleanerChoreUtil
 */
@InterfaceAudience.Private
public interface SnapshotCleanerCacheLoader {

  /**
   * Add the found files to the passed in cache. The cached file names will be compared against the
   * file names in the archive director and those file names that aren't in the cache are considered
   * valid for deletion.
   * @param fs {@link FileSystem} where the file lives
   * @param snapshot Snapshot directory to search
   * @param cache File name cache where file should be added
   * @throws IOException if there is an unexpected network error reaching the filesystem.
   */
  public void loadFiles(FileSystem fs, FileStatus snapshot, final Set<String> cache)
      throws IOException;
}
