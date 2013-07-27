package org.apache.hadoop.hbase.client;

import java.io.IOException;

public class DelegatingRetryingCallable<T> implements RetryingCallable<T> {
  protected final RetryingCallable<T> delegate;

  public DelegatingRetryingCallable(RetryingCallable<T> delegate) {
    this.delegate = delegate;
  }

  @Override
  public T call() throws Exception {
    return delegate.call();
  }

  @Override
  public void prepare(boolean reload) throws IOException {
    delegate.prepare(reload);
  }

  @Override
  public void throwable(Throwable t, boolean retrying) {
    delegate.throwable(t, retrying);
  }

  @Override
  public String getExceptionMessageAdditionalDetail() {
    return delegate.getExceptionMessageAdditionalDetail();
  }

  @Override
  public long sleep(long pause, int tries) {
    return delegate.sleep(pause, tries);
  }
}