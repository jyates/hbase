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
package org.apache.hadoop.hbase.server.error;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Helper class that just listens for process errors and propagates it to a weak-referenced list of
 * {@link ErrorListener errorListeners}.
 * <p>
 * Any {@link ErrorListener} added will only be weakly referenced, allowing easier management of
 * multiple listeners.
 * <p>
 * A single error dispatcher should be used for each set of processes to monitor. This allows for a
 * single source of truth for dispatch between all the interested parties.
 */
public class ErrorDispatcher implements ErrorListener, ErrorListenable {
  private static final Log LOG = LogFactory.getLog(ErrorDispatcher.class);
  protected List<WeakReference<ErrorListener>> listeners = new ArrayList<WeakReference<ErrorListener>>();

  @Override
  public synchronized void addErrorListener(ErrorListener failable) {
    if (failable == null) return;
    listeners.add(new WeakReference<ErrorListener>(failable));
  }

  @Override
  public synchronized void receiveError(String message, Object... info) {
    LOG.debug("Recieved error, notifying listeners...");
    for (int i = 0; i < listeners.size(); i++) {
      ErrorListener listener = listeners.get(i).get();
      // if the listener has been removed, then drop it from the list
      if (listener == null) {
        listeners.remove(i--);
        continue;
      }
      // otherwise notify the listener that we had a failure
      listener.receiveError(message, info);
    }
  }
}
