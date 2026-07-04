package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;

public class OAdminSessionEmbedded implements OAdminSession {
  private OrientDBEmbedded context;

  public OAdminSessionEmbedded(OrientDBEmbedded context) {
    this.context = context;
    // TODO Collect and manage authentication
  }

  @Override
  public OResultSet execute(String script, Map<String, Object> params) {
    return context.executeAdminStatement(script, params);
  }

  @Override
  public OResultSet execute(String script, Object... params) {
    return context.executeAdminStatement(script, params);
  }

  @Override
  public void close() {}
}
