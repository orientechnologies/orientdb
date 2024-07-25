package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;

public interface OFilterResult {
  /**
   * Filter and change a result
   *
   * @param result to check
   * @param ctx query context
   * @return a new result or null if the current result need to be skipped
   */
  OResult filterMap(OResult result, OCommandContext ctx);
}
