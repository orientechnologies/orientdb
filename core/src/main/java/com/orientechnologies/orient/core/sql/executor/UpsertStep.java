package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OAndBlock;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import com.orientechnologies.orient.core.sql.parser.OCluster;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import java.util.List;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class UpsertStep extends AbstractExecutionStep {
  private final OFromClause commandTarget;
  private final OWhereClause initialFilter;

  public UpsertStep(
      OFromClause target, OWhereClause where, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.commandTarget = target;
    this.initialFilter = where;
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream upstream = getPrev().get().syncPull(ctx);
    if (upstream.hasNext(ctx)) {
      return upstream;
    }
    return OExecutionStream.singleton(createNewRecord(ctx, commandTarget, initialFilter));
  }

  private OResult createNewRecord(
      OCommandContext ctx, OFromClause commandTarget, OWhereClause initialFilter) {
    ODocument doc;
    if (commandTarget.getItem().getIdentifier() != null) {
      doc = new ODocument(commandTarget.getItem().getIdentifier().getStringValue());
    } else if (commandTarget.getItem().getCluster() != null) {
      OCluster cluster = commandTarget.getItem().getCluster();
      Integer clusterId = cluster.getClusterNumber();
      if (clusterId == null) {
        clusterId = ctx.getDatabase().getClusterIdByName(cluster.getClusterName());
      }
      OClass clazz =
          ((ODatabaseDocumentInternal) ctx.getDatabase())
              .getMetadata()
              .getImmutableSchemaSnapshot()
              .getClassByClusterId(clusterId);
      doc = new ODocument(clazz);
    } else {
      throw new OCommandExecutionException(
          "Cannot execute UPSERT on target '" + commandTarget + "'");
    }

    OUpdatableResult result = new OUpdatableResult(doc);
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

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ INSERT (upsert, if needed)\n");
    result.append(spaces);
    result.append("  target: ");
    result.append(commandTarget);
    result.append("\n");
    result.append(spaces);
    result.append("  content: ");
    result.append(initialFilter);
    return result.toString();
  }
}
