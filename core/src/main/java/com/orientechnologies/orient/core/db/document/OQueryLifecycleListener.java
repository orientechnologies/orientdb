package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.sql.executor.OTodoResultSet;

/**
 * Created by luigidellaquila on 01/12/16.
 */
public interface OQueryLifecycleListener {

  public void queryStarted(OTodoResultSet rs);

  public void queryClosed(OTodoResultSet rs);

}
