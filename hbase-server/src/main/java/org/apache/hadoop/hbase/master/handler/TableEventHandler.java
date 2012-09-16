/**
 *
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
package org.apache.hadoop.hbase.master.handler;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Base class for performing operations against tables.
 * Checks on whether the process can go forward are done in constructor rather
 * than later on in {@link #process()}.  The idea is to fail fast rather than
 * later down in an async invocation of {@link #process()} (which currently has
 * no means of reporting back issues once started).
 */
@InterfaceAudience.Private
public abstract class TableEventHandler extends EventHandler {
  protected final MasterServices masterServices;
  protected final byte [] tableName;
  protected final String tableNameStr;
  protected boolean persistedToZk = false;

  public TableEventHandler(EventType eventType, byte [] tableName, Server server,
      MasterServices masterServices)
  throws IOException {
    super(server, eventType);
    this.masterServices = masterServices;
    this.tableName = tableName;
    this.tableNameStr = Bytes.toString(this.tableName);
  }

  /**
   * Table modifications are processed asynchronously, but provide an API for you to query their
   * status.
   * @throws IOException
   */
  public synchronized void waitForPersist() throws IOException {
    if (!persistedToZk) {
      try {
        wait();
      } catch (InterruptedException ie) {
        throw (IOException) new InterruptedIOException().initCause(ie);
      }
      assert persistedToZk;
    }
  }

  protected synchronized void setPersist() {
    if (!persistedToZk) {
      persistedToZk = true;
      notify();
    }
  }

  /**
   * @return Table descriptor for this table
   * @throws TableExistsException
   * @throws FileNotFoundException
   * @throws IOException
   */
  protected HTableDescriptor getTableDescriptor()
  throws FileNotFoundException, IOException {
    final String name = Bytes.toString(tableName);
    HTableDescriptor htd =
      this.masterServices.getTableDescriptors().get(name);
    if (htd == null) {
      throw new IOException("HTableDescriptor missing for " + name);
    }
    return htd;
  }

  byte [] hasColumnFamily(final HTableDescriptor htd, final byte [] cf)
  throws InvalidFamilyOperationException {
    if (!htd.hasFamily(cf)) {
      throw new InvalidFamilyOperationException("Column family '" +
        Bytes.toString(cf) + "' does not exist");
    }
    return cf;
  }
}
