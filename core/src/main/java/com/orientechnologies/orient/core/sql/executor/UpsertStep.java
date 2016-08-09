package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.OAndBlock;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;

import java.util.List;

/**
 * @author Luigi Dell'Aquila
 */
public class UpsertStep extends AbstractExecutionStep {
  private final OFromClause  commandTarget;
  private final OWhereClause initialFilter;

  boolean applied = false;

  public UpsertStep(OFromClause target, OWhereClause where, OCommandContext ctx) {
    super(ctx);
    this.commandTarget = target;
    this.initialFilter = where;
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (applied) {
      return getPrev().get().syncPull(ctx, nRecords);
    }
    applied = true;
    OTodoResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    if (upstream.hasNext()) {
      return upstream;
    }
    OInternalResultSet result = new OInternalResultSet();
    result.add(createNewRecord(commandTarget, initialFilter));
    return result;
  }

  private OResult createNewRecord(OFromClause commandTarget, OWhereClause initialFilter) {
    if (commandTarget.getItem().getIdentifier() == null) {
      throw new OCommandExecutionException("Cannot execute UPSERT on target '" + commandTarget + "'");
    }

    ODocument doc = new ODocument(commandTarget.getItem().getIdentifier().getStringValue());
    OResultInternal result = new OResultInternal();
    result.setElement(doc);
    if (initialFilter != null) {
      setContent(result, initialFilter);
    }
    return result;
  }

  private void setContent(OResultInternal doc, OWhereClause initialFilter) {
    List<OAndBlock> flattened = initialFilter.flatten();
    if (flattened.size() == 0) {
      return;
    }
    if (flattened.size() > 1) {
      throw new OCommandExecutionException("Cannot UPSERT on OR conditions");
    }
    OAndBlock andCond = flattened.get(0);
    for (OBooleanExpression condition : andCond.getSubBlocks()) {
      condition.transformToUpdateItem().ifPresent(x -> x.applyUpdate(doc, ctx));
    }
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }
}
