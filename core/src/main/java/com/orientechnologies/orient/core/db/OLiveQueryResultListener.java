package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.sql.executor.OResult;

/**
 * Created by tglman on 11/05/17.
 */
public interface OLiveQueryResultListener {

  void onCreate(OResult data);

  void onUpdate(OResult before, OResult after);

  void onDelete(OResult data);

  void onError();

  void onEnd();

}
