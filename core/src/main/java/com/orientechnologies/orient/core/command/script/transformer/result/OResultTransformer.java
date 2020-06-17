package com.orientechnologies.orient.core.command.script.transformer.result;

import com.orientechnologies.orient.core.sql.executor.OResult;

/** Created by Enrico Risa on 27/01/17. */
public interface OResultTransformer<T> {
  public OResult transform(T value);
}
