package com.orientechnologies.orient.core.sql.query;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.record.ORecordOperation;

/**
 * Created by luigidellaquila on 23/03/15.
 */
public interface OLiveResultListener {

  public void onLiveResult(int iLiveToken, ORecordOperation iOp) throws OException;

  public void onError(int iLiveToken);

  public void onUnsubscribe(int iLiveToken);


}
