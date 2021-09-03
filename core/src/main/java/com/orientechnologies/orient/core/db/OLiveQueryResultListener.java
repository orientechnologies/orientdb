package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;

/** Created by tglman on 11/05/17. */
public interface OLiveQueryResultListener {

  void onCreate(ODatabaseDocument database, OResult data);

  void onUpdate(ODatabaseDocument database, OResult before, OResult after);

  void onDelete(ODatabaseDocument database, OResult data);

  void onError(ODatabaseDocument database, OException exception);

  void onEnd(ODatabaseDocument database);
}
