package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;


/**
 * Exception thrown when an exception could not be created
 */
@SuppressWarnings("serial")
public class SnapshotCreationException extends IOException {

  private SnapshotDescriptor description;

  public SnapshotCreationException(String msg) {
    super(msg);
  }

  public SnapshotCreationException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public SnapshotCreationException(Throwable cause) {
    super(cause);
  }

  public SnapshotCreationException(String msg, SnapshotDescriptor desc) {
    super(msg);
    this.description = desc;
  }

  public SnapshotCreationException(Throwable cause, SnapshotDescriptor desc) {
    super(cause);
    this.description = desc;
  }

  public SnapshotCreationException(String msg, Throwable cause,
      SnapshotDescriptor desc) {
    super(msg, cause);
    this.description = desc;
  }

  public SnapshotDescriptor getSnapshotDescription() {
    return this.description;
  }
}
