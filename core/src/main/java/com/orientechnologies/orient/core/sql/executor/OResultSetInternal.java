package com.orientechnologies.orient.core.sql.executor;

public interface OResultSetInternal extends OResultSet {

  public boolean isExplain();

  public boolean isDetached();
}
