package com.orientechnologies.agent;

import com.orientechnologies.orient.core.metadata.security.ORule;

/** Created by Enrico Risa on 09/04/15. */
public class DatabaseProfilerResource extends ORule.ResourceGeneric {

  public static final String DATABASE_PROFILING = "database.profiling";
  public static final String PROFILER = "PROFILER";

  public DatabaseProfilerResource() {
    super(PROFILER, DATABASE_PROFILING);
  }
}
