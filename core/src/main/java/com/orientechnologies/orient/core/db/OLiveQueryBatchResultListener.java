package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

public interface OLiveQueryBatchResultListener extends OLiveQueryResultListener{

  void onBatchEnd(ODatabaseDocument database);

}
