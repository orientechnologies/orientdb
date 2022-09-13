package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

/** Created by luigidellaquila on 02/11/16. */
public class OTraverseResult extends OResultInternal {
  protected Integer depth;

  public OTraverseResult() {}

  public OTraverseResult(OIdentifiable element) {
    super(element);
  }

  @Override
  public <T> T getProperty(String name) {
    if ("$depth".equalsIgnoreCase(name)) {
      return (T) depth;
    }
    return super.getProperty(name);
  }

  @Override
  public void setProperty(String name, Object value) {
    if ("$depth".equalsIgnoreCase(name)) {
      if (value instanceof Number) {
        depth = ((Number) value).intValue();
      }
    } else {
      super.setProperty(name, value);
    }
  }
}
