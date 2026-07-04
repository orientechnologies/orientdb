package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;

public interface OAdminSession extends AutoCloseable {

  OResultSet execute(String script, Map<String, Object> params);

  OResultSet execute(String script, Object... params);

  void close();
}
