package com.orientechnologies.orient.core;

/**
 * @author Sergey Sitnikov
 * @since 22/03/16
 */
public interface OUncompletedCommit<R> {

  class NoOperation<R> implements OUncompletedCommit<R> {
    private final R result;

    public NoOperation(R result) {
      this.result = result;
    }

    @Override
    public R complete() {
      // do nothing
      return result;
    }

    @Override
    public void rollback() {
      // do nothing
    }
  }

  OUncompletedCommit<Void> NO_OPERATION = new NoOperation<Void>(null);

  R complete();

  void rollback();

}
