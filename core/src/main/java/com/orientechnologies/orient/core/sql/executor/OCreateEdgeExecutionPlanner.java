package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class OCreateEdgeExecutionPlanner {

  protected OIdentifier targetClass;
  protected OIdentifier targetClusterName;
  protected OExpression leftExpression;
  protected OExpression rightExpression;

  protected OInsertBody body;
  protected Number      retry;
  protected Number      wait;
  protected OBatch      batch;


  public OCreateEdgeExecutionPlanner(OCreateEdgeStatement statement) {
    this.targetClass = statement.getTargetClass() == null ? null : statement.getTargetClass().copy();
    this.targetClusterName = statement.getTargetClusterName() == null ? null : statement.getTargetClusterName().copy();
    this.leftExpression = statement.getLeftExpression() == null ? null : statement.getLeftExpression().copy();
    this.rightExpression = statement.getRightExpression() == null ? null : statement.getRightExpression().copy();
    this.body = statement.getBody() == null ? null : statement.getBody().copy();
    this.retry = statement.getRetry();
    this.wait = statement.getWait();
    this.batch = statement.getBatch() == null ? null : statement.getBatch().copy();

  }

  public OInsertExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {

    if (targetClass == null) {
      if (targetClusterName == null) {
        targetClass = new OIdentifier("E");
      } else {
        ODatabase db = ctx.getDatabase();
        OClass clazz = db.getMetadata().getSchema().getClassByClusterId(db.getClusterIdByName(targetClusterName.getStringValue()));
        if (clazz != null) {
          targetClass = new OIdentifier(clazz.getName());
        } else {
          targetClass = new OIdentifier("E");
        }
      }
    }

    OInsertExecutionPlan result = new OInsertExecutionPlan(ctx);

    handleCheckType(result, ctx, enableProfiling);

    handleGlobalLet(result, new OIdentifier("$__ORIENT_CREATE_EDGE_fromV"), leftExpression, ctx, enableProfiling);
    handleGlobalLet(result, new OIdentifier("$__ORIENT_CREATE_EDGE_toV"), rightExpression, ctx, enableProfiling);

    result.chain(new CreateEdgesStep(targetClass, targetClusterName, new OIdentifier("$__ORIENT_CREATE_EDGE_fromV"),
        new OIdentifier("$__ORIENT_CREATE_EDGE_toV"), wait, retry, batch, ctx, enableProfiling));

    handleSetFields(result, body, ctx, enableProfiling);
    handleSave(result, targetClusterName, ctx, enableProfiling);
    //TODO implement batch, wait and retry
    return result;
  }

  private void handleGlobalLet(OInsertExecutionPlan result, OIdentifier name, OExpression expression, OCommandContext ctx, boolean profilingEnabled) {
    result.chain(new GlobalLetExpressionStep(name, expression, ctx, profilingEnabled));
  }

  private void handleCheckType(OInsertExecutionPlan result, OCommandContext ctx, boolean profilingEnabled) {
    if (targetClass != null) {
      result.chain(new CheckClassTypeStep(targetClass.getStringValue(), "E", ctx, profilingEnabled));
    }
  }

  private void handleSave(OInsertExecutionPlan result, OIdentifier targetClusterName, OCommandContext ctx, boolean profilingEnabled) {
    result.chain(new SaveElementStep(ctx, targetClusterName, profilingEnabled));
  }

  private void handleSetFields(OInsertExecutionPlan result, OInsertBody insertBody, OCommandContext ctx, boolean profilingEnabled) {
    if (insertBody == null) {
      return;
    }
    if (insertBody.getIdentifierList() != null) {
      result.chain(new InsertValuesStep(insertBody.getIdentifierList(), insertBody.getValueExpressions(), ctx, profilingEnabled));
    } else if (insertBody.getContent() != null) {
      result.chain(new UpdateContentStep(insertBody.getContent(), ctx, profilingEnabled));
    } else if (insertBody.getSetExpressions() != null) {
      List<OUpdateItem> items = new ArrayList<>();
      for (OInsertSetExpression exp : insertBody.getSetExpressions()) {
        OUpdateItem item = new OUpdateItem(-1);
        item.setOperator(OUpdateItem.OPERATOR_EQ);
        item.setLeft(exp.getLeft().copy());
        item.setRight(exp.getRight().copy());
        items.add(item);
      }
      result.chain(new UpdateSetStep(items, ctx, profilingEnabled));
    }
  }

}