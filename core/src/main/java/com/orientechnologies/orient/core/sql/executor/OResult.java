package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.Set;

/**
 * Created by luigidellaquila on 21/07/16.
 */
public interface OResult {
  <T> T getProperty(String name);

  Set<String> getPropertyNames();

  boolean isElement();

  OIdentifiable getElement();

  OIdentifiable toElement();
}
