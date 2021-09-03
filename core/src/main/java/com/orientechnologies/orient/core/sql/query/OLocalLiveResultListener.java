package com.orientechnologies.orient.core.sql.query;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.ORecordOperation;

/** Created by luigidellaquila on 23/03/15. */
public class OLocalLiveResultListener implements OLiveResultListener, OCommandResultListener {

  private final OLiveResultListener underlying;

  protected OLocalLiveResultListener(OLiveResultListener underlying) {
    this.underlying = underlying;
  }

  @Override
  public boolean result(Object iRecord) {
    return false;
  }

  @Override
  public void end() {}

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public void onLiveResult(int iLiveToken, ORecordOperation iOp) throws OException {
    underlying.onLiveResult(iLiveToken, iOp);
  }

  @Override
  public void onError(int iLiveToken) {
    underlying.onError(iLiveToken);
  }

  @Override
  public void onUnsubscribe(int iLiveToken) {
    underlying.onUnsubscribe(iLiveToken);
  }
}
