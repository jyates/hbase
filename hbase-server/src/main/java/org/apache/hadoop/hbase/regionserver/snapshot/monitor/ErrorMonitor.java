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
package org.apache.hadoop.hbase.regionserver.snapshot.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Helper class to propate error checking class and keep track of the simple
 * error state.
 * @param <T> class that is using this error monitor
 */
public class ErrorMonitor<T> implements SnapshotErrorMonitor {

  private static final Log LOG = LogFactory.getLog(ErrorMonitor.class);
  private final Class<T> clazz;
  private volatile boolean error;

  public ErrorMonitor(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public boolean checkForError() {
    LOG.debug("Class:" + clazz + " checking for error");
    return this.error;
  }

  public void setError(String msg) {
    LOG.error(this.clazz + " is setting snapshot error becuase:" + msg);
    this.error = true;
  }

  @Override
  public Class<T> getCaller() {
    return this.clazz;
  }
}
