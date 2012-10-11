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
package org.apache.hadoop.hbase.master.snapshot.manage;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.snapshot.TableSnapshotHandler;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionOrchestrator;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionSnare;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test basic snapshot manager functionality
 */
@Category(SmallTests.class)
public class TestSnapshotManager {
  private static final Log LOG = LogFactory.getLog(TestSnapshotManager.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  MasterServices services = Mockito.mock(MasterServices.class);
  ZooKeeperWatcher watcher = Mockito.mock(ZooKeeperWatcher.class);

  private SnapshotManager getNewManager() throws KeeperException {
    Mockito.reset(services, watcher);
    return new SnapshotManager(services, watcher);
  }

  @Test
  public void testNoSnapshotDirectoryExistsFailure() throws Exception {
    FileSystem fs = UTIL.getTestFileSystem();
    SnapshotManager manager = getNewManager();
    Path root = UTIL.getDataTestDir();
    Path snapshotDir = new Path(root, ".snapshot");
    Path tmpDir = new Path(snapshotDir, ".tmp");
    Path workingDir = new Path(tmpDir, "not_a_snapshot");
    // no failure when we don't have a handler
    manager.completeSnapshot(snapshotDir, workingDir, fs);

    TableSnapshotHandler handler = Mockito.mock(TableSnapshotHandler.class);
    manager.setSnapshotHandler(handler);
    try {
      manager.completeSnapshot(snapshotDir, workingDir, fs);
      fail("Manager shouldn't successfully complete move of a non-existent directory.");
    } catch (IOException e) {
      LOG.info("Correctly failed to move non-existant directory: " + e.getMessage());
    }
    // make sure we told the handler to finish
    Mockito.verify(handler, Mockito.times(1)).finish();
  }

  @Test
  public void testInProcess() throws KeeperException, SnapshotCreationException {
    SnapshotManager manager = getNewManager();
    TableSnapshotHandler handler = Mockito.mock(TableSnapshotHandler.class);
    assertFalse("Manager is in process when there is no current handler", manager.isInProcess());
    manager.setSnapshotHandler(handler);
    Mockito.when(handler.getFinished()).thenReturn(false);
    assertTrue("Manager isn't in process when handler is running", manager.isInProcess());
    Mockito.when(handler.getFinished()).thenReturn(true);
    assertFalse("Manager is process when handler isn't running", manager.isInProcess());
  }

  @Test
  public void testAbortPropagation() throws Exception {
    // create a new orchestrator and hook up a listener
    SnapshotManager manager = getNewManager();
    FSUtils.setRootDir(UTIL.getConfiguration(), UTIL.getDataTestDir());

    // setup a mock snapshot to run
    String tableName = "some table";
    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName("testAbort")
        .setTable(tableName).build();
    Server parent = Mockito.mock(Server.class);
    Mockito.when(parent.getConfiguration()).thenReturn(UTIL.getConfiguration());
    TableDescriptors tables = Mockito.mock(TableDescriptors.class);
    Mockito.when(services.getTableDescriptors()).thenReturn(tables);
    HTableDescriptor descriptor = Mockito.mock(HTableDescriptor.class);
    Mockito.when(tables.get(Mockito.anyString())).thenReturn(descriptor);
    MasterFileSystem mfs = Mockito.mock(MasterFileSystem.class);
    Mockito.when(mfs.getFileSystem()).thenReturn(UTIL.getTestFileSystem());
    Mockito.when(services.getMasterFileSystem()).thenReturn(mfs);
    Mockito.when(services.getConfiguration()).thenReturn(UTIL.getConfiguration());

    // set our own exception snare too
    ExceptionOrchestrator<HBaseSnapshotException> orchestrator = manager.getExceptionOrchestrator();
    ExceptionSnare<HBaseSnapshotException> snare = new ExceptionSnare<HBaseSnapshotException>();
    orchestrator.addErrorListener(orchestrator.genericVisitor, snare);

    // create a new handler that we will check for errors
    TableSnapshotHandler handler = manager.newDisabledTableSnapshotHandler(snapshot, parent);
    manager.abort("some reason", new Exception("some exception"));
    assertTrue("Snare didn't receive error notification from snapshot manager.",
      handler.checkForError());
    assertTrue("Snare didn't receive error notification from snapshot manager.",
      snare.checkForError());
  }
}