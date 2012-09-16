package org.apache.hadoop.hbase.server.snapshot.error;

import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionListener;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;

/**
 * Marker interface for a class that can coordinates error propagation, but also can receive generic
 * snapshot error notifications.
 */
public interface SnapshotErrorListener extends ExceptionListener<HBaseSnapshotException>,
    ExceptionCheckable<HBaseSnapshotException>,
    SnapshotFailureListener {
}