package com.orientechnologies.orient.core.sql.executor;

/** Created by Enrico Risa */
public interface OQueryMetrics {

  String getStatement();

  long getStartTime();

  long getElapsedTimeMillis();

  String getLanguage();
}
