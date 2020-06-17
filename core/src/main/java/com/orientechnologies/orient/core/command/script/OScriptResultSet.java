package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.sql.executor.OIteratorResultSet;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.Iterator;

/**
 * Wrapper of OIteratorResultSet Used in script results with conversion to OResult for single
 * iteration Created by Enrico Risa on 27/01/17.
 */
public class OScriptResultSet extends OIteratorResultSet {

  protected OScriptTransformer transformer;

  public OScriptResultSet(Iterator iter, OScriptTransformer transformer) {
    super(iter);
    this.transformer = transformer;
  }

  @Override
  public OResult next() {

    Object next = iterator.next();
    return transformer.toResult(next);
  }
}
