package com.orientechnologies.orient.core.command.script.transformer;

import com.orientechnologies.orient.core.command.script.transformer.result.OResultTransformer;
import com.orientechnologies.orient.core.command.script.transformer.resultset.OResultSetTransformer;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

/** Created by Enrico Risa on 27/01/17. */
public interface OScriptTransformer {
  OResultSet toResultSet(Object value);

  OResult toResult(Object value);

  boolean doesHandleResult(Object value);

  void registerResultTransformer(Class clazz, OResultTransformer resultTransformer);

  void registerResultSetTransformer(Class clazz, OResultSetTransformer transformer);
}
