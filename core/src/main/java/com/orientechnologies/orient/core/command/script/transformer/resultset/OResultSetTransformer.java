package com.orientechnologies.orient.core.command.script.transformer.resultset;

import com.orientechnologies.orient.core.sql.executor.OResultSet;

/** Created by Enrico Risa on 27/01/17. */
public interface OResultSetTransformer<T> {

  public OResultSet transform(T value);
}
