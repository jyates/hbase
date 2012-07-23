package org.apache.hadoop.hbase.snapshot;

/**
 * Exception thrown when we get a snapshot error about a snapshot we don't know or recognize.
 */
@SuppressWarnings("serial")
public class UnknownSnapshotException extends SnapshotCreationException {

	public UnknownSnapshotException(String msg) {
		super(msg);
	}

}
