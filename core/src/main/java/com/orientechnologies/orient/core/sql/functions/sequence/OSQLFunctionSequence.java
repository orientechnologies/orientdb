package com.orientechnologies.orient.core.sql.functions.sequence;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionConfigurableAbstract;

/**
 * Returns a sequence by name.
 * 
 * @author Luca Garulli
 */
public class OSQLFunctionSequence extends OSQLFunctionConfigurableAbstract {
  public static final String NAME = "sequence";

  public OSQLFunctionSequence() {
    super(NAME, 1, 1);
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext) {
    final String seqName;
    if (configuredParameters[0] instanceof OSQLFilterItem)
      seqName = (String) ((OSQLFilterItem) configuredParameters[0]).getValue(iCurrentRecord, iCurrentResult, iContext);
    else
      seqName = configuredParameters[0].toString();

    return ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSequenceLibrary().getSequence(seqName);
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public String getSyntax() {
    return "sequence(<name>)";
  }

  @Override
  public boolean aggregateResults() {
    return false;
  }

}
