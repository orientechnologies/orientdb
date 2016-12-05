package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.sql.parser.OLocalResultSetLifecycleDecorator;

/**
 * Created by luigidellaquila on 01/12/16.
 */
public interface OQueryLifecycleListener {

  public void queryStarted(OLocalResultSetLifecycleDecorator rs);

  public void queryClosed(OLocalResultSetLifecycleDecorator rs);

}
