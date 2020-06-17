package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.sql.executor.OResultSet;

/** Created by luigidellaquila on 01/12/16. */
public interface OQueryLifecycleListener {

  void queryStarted(String id, OResultSet resultSet);

  void queryClosed(String id);
}
