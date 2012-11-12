package com.orientechnologies.orient.core.sql.functions.misc;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Returns the first <code>field/value</code> not null parameter. if no <code>field/value</code> is <b>not</b> null, returns null.
 * 
 * <p>
 * Syntax: <blockquote>
 * 
 * <pre>
 * coalesce(&lt;field|value&gt;[,&lt;field|value&gt;]*)
 * </pre>
 * 
 * </blockquote>
 * 
 * <p>
 * Examples: <blockquote>
 * 
 * <pre>
 * SELECT <b>coalesce('a', 'b')</b> FROM ...
 *  -> 'a'
 * 
 * SELECT <b>coalesce(null, 'b')</b> FROM ...
 *  -> 'b'
 * 
 * SELECT <b>coalesce(null, null, 'c')</b> FROM ...
 *  -> 'c'
 * 
 * SELECT <b>coalesce(null, null)</b> FROM ...
 *  -> null
 * 
 * </pre>
 * 
 * </blockquote>
 * 
 * @author Claudio Tesoriero
 */

public class OSQLFunctionCoalesce extends OSQLFunctionAbstract {
  public static final String NAME = "coalesce";

  public OSQLFunctionCoalesce() {
    super(NAME, 1, 1000);
  }

  @Override
  public Object execute(OIdentifiable iCurrentRecord, ODocument iCurrentResult, final Object[] iParameters, OCommandContext iContext) {
    int length = iParameters.length;
    for (int i = 0; i < length; i++) {
      if (iParameters[i] != null)
        return iParameters[i];
    }
    return null;
  }

  @Override
  public String getSyntax() {
    return "Returns the first not-null parameter or null if all parameters are null. Syntax: coalesce(<field|value> [,<field|value>]*)";
  }
}
