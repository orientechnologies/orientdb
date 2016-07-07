package com.orientechnologies.orient.core.sql.executor;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public class OResult {
  Map<String, Object> content = new HashMap<>();

  public void setProperty(String name, Object value) {
    content.put(name, value);
  }

  public Object getProperty(String name) {
    return content.get(name);
  }
}
