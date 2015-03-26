package com.orientechnologies.orient.core.query.live;

import com.orientechnologies.orient.core.db.record.ORecordOperation;

/**
 * Created by luigidellaquila on 16/03/15.
 */
public interface OLiveQueryListener {

  public void onLiveResult(ORecordOperation iRecord);

  public void onLiveResultEnd();
}
