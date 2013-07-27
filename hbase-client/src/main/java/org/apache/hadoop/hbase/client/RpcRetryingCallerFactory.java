package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * Factory to create an {@link RpcRetryingCaller}
 */
public class RpcRetryingCallerFactory {

  /** Configuration key for a custom {@link RpcRetryingCaller} */
  public static final String CUSTOM_CALLER_CONF_KEY = "hbase.rpc.caller.clazz";
  protected Configuration conf;

  public RpcRetryingCallerFactory(Configuration conf) {
    this.conf = conf;
  }

  public <T> RpcRetryingCaller<T> newCaller() {
    return new RpcRetryingCaller<T>(conf);
  }

  public static RpcRetryingCallerFactory instantiate(Configuration configuration) {
    String rpcCallerClazz =
        configuration.get(RpcRetryingCallerFactory.CUSTOM_CALLER_CONF_KEY,
          RpcRetryingCaller.class.getName());
    return ReflectionUtils.instantiateWithCustomCtor(rpcCallerClazz,
      new Class[] { Configuration.class }, new Object[] { configuration });
  }
}