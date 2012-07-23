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
package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionSnapshotUtils;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Snapshot utilities for clients, regionservers, and masters.
 */
public class GlobalSnapshotUtils {

  /**
   * List all the HFiles in the given table
   * @param fs FileSystem where the table lives
   * @param tableDir directory of the table
   * @return array of the current HFiles in the table (could be a zero-length array)
   * @throws IOException on unexecpted error reading the FS
   */
  public static FileStatus[] listHFiles(FileSystem fs, Path tableDir) throws IOException {
    // setup the filters we will need based on the filesystem
    final PathFilter dirFilter = new FSUtils.DirFilter(fs);
    PathFilter familyDirectory = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return dirFilter.accept(path) && !path.getName().startsWith(".");
      }
    };
    PathFilter fileFilter = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return !dirFilter.accept(path);
      }
    };

    FileStatus[] regionDirs = FSUtils.listStatus(fs, tableDir, dirFilter);
    // if no regions, then we are done
    if (regionDirs == null || regionDirs.length == 0) return new FileStatus[0];

    // go through each of the regions, and add al the hfiles under each family
    List<FileStatus> regionFiles = new ArrayList<FileStatus>(regionDirs.length);
    for (FileStatus regionDir : regionDirs) {
      FileStatus[] fams = FSUtils.listStatus(fs, regionDir.getPath(), familyDirectory);
      // if no families, then we are done again
      if (fams == null || fams.length == 0) continue;
      // add all the hfiles under the family
      regionFiles.addAll(RegionSnapshotUtils.getHFilesInRegion(fams, fs, fileFilter));
    }
    FileStatus[] files = new FileStatus[regionFiles.size()];
    regionFiles.toArray(files);
    return files;
  }

}