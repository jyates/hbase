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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * Base utility class for listening to WAL actions.
 * <p>
 * Subclasses should override methods only as needed.
 */
public class BaseWALActionsListener implements WALActionsListener {

  @Override
  public void preLogRoll(Path oldPath, Path newPath) throws IOException {
    // TODO Implement preLogRoll

  }

  @Override
  public void postLogRoll(Path oldPath, Path newPath) throws IOException {
    // TODO Implement postLogRoll

  }

  @Override
  public void preLogArchive(Path oldPath, Path newPath) throws IOException {
    // TODO Implement preLogArchive

  }

  @Override
  public void postLogArchive(Path oldPath, Path newPath) throws IOException {
    // TODO Implement postLogArchive

  }

  @Override
  public void logRollRequested() {
    // TODO Implement logRollRequested

  }

  @Override
  public void logCloseRequested() {
    // TODO Implement logCloseRequested

  }

  @Override
  public void visitLogEntryBeforeWrite(HTableDescriptor htd, HLogKey logKey, WALEdit logEdit) {
    // TODO Implement visitLogEntryBeforeWrite

  }

  @Override
  public void visitLogEntryBeforeWrite(HRegionInfo info, HLogKey logKey, WALEdit logEdit) {
    // TODO Implement visitLogEntryBeforeWrite

  }

}
