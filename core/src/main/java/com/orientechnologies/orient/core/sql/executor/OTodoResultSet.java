package com.orientechnologies.orient.core.sql.executor;

/**
 * Created by luigidellaquila on 07/07/16.
 */
public interface OTodoResultSet {

  public boolean hasNext();

  public OResult next();

  void close();

}
