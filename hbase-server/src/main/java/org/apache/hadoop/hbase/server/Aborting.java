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
package org.apache.hadoop.hbase.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;

/**
 * A simple {@link Abortable} that manages whether or not the object has been aborted yet.
 * <p>
 * Subclasses should ensure that they call {@link #abort(String, Throwable)} in this class when
 * overriding the abort method.
 */
public class Aborting implements Abortable {

  private volatile boolean aborted;
  private static final Log LOG = LogFactory.getLog(Aborting.class);

  @Override
  public void abort(String why, Throwable e) {
    if (this.aborted) return;
    this.aborted = true;
    LOG.warn("Aborting because: " + why, e);
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }

}
