/*
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
/**
 * <h1>Snapshot Error Handling</h1>
 * <p>
 * Each snapshot task - take, restore etc. - has an overall goal (take a snapshot of the table,
 * restore a table from a snapshot, etc.) which is composed of multiple subtasks. For instance, in
 * taking a snapshot, we have a subtask to snapshot each region (so for n regions, there are n
 * 'snapshot region' subtasks).
 * <p>
 * If any subtask fails, we want to propagate that error to all the subtasks in the overall task so
 * they, and the overall task, can quickly kill themselves. To accomplish this we use a single
 * shared {@link org.apache.hadoop.hbase.server.errorhandling.notification.ErrorCheckable} between
 * the running task and its subtasks.
 * <p>
 * We only care about the first thing that caused the task to fail - its likely that one subtasks'
 * failure will cause another subtask to fail. This actually boils down to needing the per-task
 * {@link org.apache.hadoop.hbase.server.errorhandling.notification.ErrorCheckable} to answer one
 * question, "can I keep going or should I stop?"
 * <p>
 * <h2>Cross-Thread Error Handling</h2>
 * <p>
 * The task may fail due to outside reasons; each task has an error monitor to keep track of two
 * things:
 * <ol>
 * <li>If the local task has a failure</li>
 * <li>If there is an external failure that should cause this task to stop</li>
 * </ol>
 * A task could fail locally due to things like an {@link java.io.IOException} or by taking too long
 * and timing out. However, a task could also fail due to outside reasons, like the server running
 * the task is shutting down.
 * <p>
 * Therefore we need to have a way to communicate between the main orchestrator of the snapshot, on
 * the master this is the {@link org.apache.hadoop.hbase.master.snapshot.manage.SnapshotManager},
 * and each of the subtasks. This communication is handled via a
 * {@link javax.management.NotificationBroadcaster} running on the SnapshotManager that passes
 * special notifications to each of the listening snapshot tasks when there is an external error.
 * <ul>
 * <li>NOTE: This is actually implemented as a
 * {@link org.apache.hadoop.hbase.server.errorhandling.notification.WeakReferencingNotificationBroadcaster} to
 * ensure that we don't leak references to tasks.</li>
 * </ul>
 * <p>
 * To ensure that each snapshot task is bound to the
 * {@link org.apache.hadoop.hbase.master.snapshot.manage.SnapshotManager}'s exception broadcasts, we
 * use a
 * {@link org.apache.hadoop.hbase.server.errorhandling.notification.snapshot.SnapshotExceptionMonitorFactory}
 * to create the {@link org.apache.hadoop.hbase.server.errorhandling.notification.ErrorCheckable}
 * for each task. The factory ensures that each handler is bound to the main broadcaster.
 * <p>
 * <h2>Handling Snapshot Exceptions</h2>
 * <p>
 * All failures to running snapshot tasks (or subtasks) are passed into
 * {@link org.apache.hadoop.hbase.server.errorhandling.notification.snapshot.SnapshotExceptionSnare} which
 * ensures that all exception
 */

package org.apache.hadoop.hbase.server.errorhandling.notification.snapshot;