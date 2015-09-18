package com.orientechnologies.orient.core.sql.method.misc;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 *
 * @author Matan Shukry
 */
public class OSQLMethodEach extends OAbstractSQLMethod {
  public static final String NAME = "each";
  public static final String VARIABLE_PROCESS_EACH = "$processEach";

  public OSQLMethodEach() {
    super(NAME, 0, 1);
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, OCommandContext iContext, Object ioResult, Object[] iParams) {
    Boolean processEach = Boolean.TRUE;
    if (iParams != null && iParams.length >= 1) {
      processEach = Boolean.valueOf(iParams[0].toString());
    }
    iContext.setVariable(VARIABLE_PROCESS_EACH, processEach);
    return iThis;
  }
}
