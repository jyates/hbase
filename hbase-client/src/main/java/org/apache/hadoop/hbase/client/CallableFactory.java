package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;

/**
 * Create {@link ServerCallableImpl}s for the client.
 */
public class CallableFactory {

  protected HConnection hconnection;
  protected byte[] tableName;
  protected int operationTimeout;

  public CallableFactory(HConnection connection, byte[] tableName, int operationTimeout) {
    this.hconnection = connection;
    this.tableName = tableName;
    this.operationTimeout = operationTimeout;
  }

  public CallableFactory(HConnection connection, byte[] tableName) {
    this(connection, tableName, HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
  }

  public ScannerCallable newScannerCallable(Scan scan, ScanMetrics metrics) throws IOException {
    return new ScannerCallable(hconnection, tableName, scan, metrics);
  }

  public <T> ServerCallable<T> newCallable(byte[] row) throws IOException {
    return new ServerCallableImpl<T>(hconnection, tableName, row, operationTimeout);
  }

  public ServerCallable<MultiResponse>
      newMultiCallable(HRegionLocation loc, MultiAction<Row> multi) throws IOException {
    return new MultiServerCallable<Row>(hconnection, tableName, loc, multi);
  }
}