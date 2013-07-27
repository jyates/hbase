package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HBaseAdmin.MasterCallable;

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommand.Setter;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;

public class CircuitBreakingRpcCallerFactory extends RpcRetryingCallerFactory {
  /** Group key for the connections to the regionservers */
  private static final String REGIONSERVER_CONNECTION_KEY =
      "hbase.rpc.caller.factory.hystrix.regionserver-key";

  // this is probably better with an enum, but the moment a hashmap is just easier
  @SuppressWarnings("rawtypes")
  private Map<Class<? extends RetryingCallable>, CallableWrapper> wrappers =
      new HashMap<Class<? extends RetryingCallable>, CallableWrapper>();

  public CircuitBreakingRpcCallerFactory(Configuration conf) {
    super(conf);
    //setup the wrappers
    this.wrappers.put(RegionServerCallable.class, new RegionServerCallableWrapper());
    this.wrappers.put(MasterCallable.class, new MasterCallableWrapper());
  }

  @Override
  public <T> RpcRetryingCaller<T> newCaller() {
    return new CircuitBrokenRpcRetryingCaller<T>(conf);
  }

  /**
   * RpcRetryingCaller that uses a circuit-breaker per regionserver.
   */
  public class CircuitBrokenRpcRetryingCaller<T> extends RpcRetryingCaller<T> {

    @Override
    public synchronized T callWithRetries(RetryingCallable<T> callable, int callTimeout)
        throws IOException, RuntimeException {
      // wrap the callable with one that uses a circuit breaker
      return super.callWithRetries(wrap(callable), callTimeout);
    }

    @Override
    public T callWithoutRetries(RetryingCallable<T> callable) throws IOException, RuntimeException {
      return super.callWithoutRetries(wrap(callable));
    }

    public CircuitBrokenRpcRetryingCaller(Configuration conf) {
      super(conf);
    }

    private RetryingCallable<T> wrap(RetryingCallable<T> callable) {
      CallableWrapper wrapper = wrappers.get(callable.getClass());
      if (wrapper != null) {
        callable = wrapper.wrap(callable);
      }
      return callable;
    }
  }

  /**
   * Simple interface to wrap calls to a {@link RetryingCallable}
   */
  private interface CallableWrapper {
    <T> RetryingCallable<T> wrap(RetryingCallable<T> callable);
  }

  // TODO For the moment, not implementing this. Its a bit tricky as we need to know the exact
  // master we are talking to, which isn't directly available from the hconnection.
  private class MasterCallableWrapper implements CallableWrapper {
    @Override
    public <T> RetryingCallable<T> wrap(RetryingCallable<T> c) {
      return c;
    }
  }

  private class RegionServerCallableWrapper implements CallableWrapper {
    @Override
    public <T> RetryingCallable<T> wrap(RetryingCallable<T> c) {
      RegionServerCallable<T> callable = (RegionServerCallable<T>) c;
      return new CircuitBreakerRegionServerCallable<T>(callable);
    }

    private class CircuitBreakerRegionServerCallable<R> extends
        DelegatingRetryingCallable<R, RegionServerCallable<R>> {

      private HystrixCommand<R> command;

      public CircuitBreakerRegionServerCallable(RegionServerCallable<R> delegate) {
        super(delegate);
      }

      @Override
      public void prepare(boolean reload) throws IOException {
        // get the region location
        delegate.prepare(reload);
        // settup the command to guard the call
        Setter setter = getCommandSetter(delegate.getConnection(), delegate.getTableName(),
          delegate.getRow());
        this.command = new HystrixCommand<R>(setter) {
          @Override
          public R run() throws Exception {
            return CircuitBreakerRegionServerCallable.this.delegate.call();
          }
        };
      }

      @Override
      public R call() throws IOException {
        // TODO set the right group key
        return this.command.execute();
      }
    }
  }

  private static Setter getCommandSetter(HConnection connection, byte[] tableName, byte[] row)
      throws IOException {
    HRegionLocation location = connection.getRegionLocation(tableName, row, false);
    return Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(REGIONSERVER_CONNECTION_KEY))
        .andCommandKey(HystrixCommandKey.Factory.asKey(location.getServerName().toShortString()));
  }
}