package com.orientechnologies.orient.core;

/**
 * @author Sergey Sitnikov
 * @since 22/03/16
 */
public interface OUnfinishedCommit {

  OUnfinishedCommit NO_OPERATION = new OUnfinishedCommit() {
    @Override
    public void complete() {
      // do nothing
    }

    @Override
    public void cancel() {
      // do nothing
    }
  };

  void complete();

  void cancel();

}
