package org.apache.hadoop.hbase.snapshot.error;

import org.apache.hadoop.hbase.server.error.ErrorConverter;
import org.apache.hadoop.hbase.server.error.ErrorListener;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptor;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;

public class SnapshotErrorConverter implements
    ErrorConverter<SnapshotFailureListener, SnapshotCreationException> {

  private final SnapshotDescriptor snapshot;

  public SnapshotErrorConverter(SnapshotDescriptor desc) {
    this.snapshot = desc;
  }

  @Override
  public ErrorListener wrap(final SnapshotFailureListener delegate) {
    return new ErrorListener(){
      @Override
      public void receiveError(String message, Object... info) {
        delegate.snapshotFailure(message, SnapshotErrorConverter.convert(info));
      }};
  }

  @Override
  public SnapshotCreationException convertToException(String message, Object... errorInfo) {
    SnapshotDescriptor desc = convert(errorInfo);
    if (desc == null) return new UnknownSnapshotException("Expected error about snapshot:"
        + this.snapshot + ".Can't understand snapshot error info:" + errorInfo
        + ". Failed because:" + message);
    return new SnapshotCreationException(message, desc);
  }

  @Override
  public boolean shouldRecieveError(Object... info) {
    SnapshotDescriptor desc = convert(info);
    return this.snapshot.equals(desc);
  }

  /**
   * Convert the error information to a snapshot description
   * @param info information returned from an error notification
   * @return the passed snapshot descriptor
   */
  private static SnapshotDescriptor convert(Object[] info) {
    if (info == null || info.length != 1) return null;
    return (SnapshotDescriptor) info[0];
  }
}