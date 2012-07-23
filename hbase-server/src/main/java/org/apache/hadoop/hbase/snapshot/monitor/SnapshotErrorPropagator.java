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
package org.apache.hadoop.hbase.snapshot.monitor;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;

/**
 * Helper class that just listens for snapshot errors and propagates it to a
 * weak-referenced list of {@link SnapshotFailureListener failureListeners}.
 * <p>
 * Any {@link SnapshotFailureListener} added will only be weakly referenced,
 * allowing easier management of multiple listeners
 */
public class SnapshotErrorPropagator implements SnapshotFailureListener, SnapshotFailureListenable {
  private static final Log LOG = LogFactory.getLog(SnapshotErrorPropagator.class);
  protected List<WeakReference<SnapshotFailureListener>> listeners = new ArrayList<WeakReference<SnapshotFailureListener>>();

  @Override
  public void addSnapshotFailureListener(SnapshotFailureListener failable) {
    if (failable == null) return;
    listeners.add(new WeakReference<SnapshotFailureListener>(failable));
  }

  @Override
  public synchronized void snapshotFailure(SnapshotDescriptor snapshot, String description) {
    LOG.debug("Recieved snapshot failure, notifying listeners...");
    for (int i = 0; i < listeners.size(); i++) {
      SnapshotFailureListener listener = listeners.get(i).get();
      // if the listener has been removed, then drop it from the list
      if (listener == null) {
        listeners.remove(i--);
        continue;
      }
      // otherwise notify the listener that we had a failure
      listener.snapshotFailure(snapshot, description);
    }
  }
}
