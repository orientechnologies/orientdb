package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class OInsertExecutionPlanner {

  protected OIdentifier      targetClass;
  protected OIdentifier      targetClusterName;
  protected OCluster         targetCluster;
  protected OIndexIdentifier targetIndex;
  protected OInsertBody      insertBody;
  protected OProjection      returnStatement;
  protected OSelectStatement selectStatement;

  public OInsertExecutionPlanner() {

  }

  public OInsertExecutionPlanner(OInsertStatement statement) {
    this.targetClass = statement.getTargetClass() == null ? null : statement.getTargetClass().copy();
    this.targetClusterName = statement.getTargetClusterName() == null ? null : statement.getTargetClusterName().copy();
    this.targetCluster = statement.getTargetCluster() == null ? null : statement.getTargetCluster().copy();
    this.targetIndex = statement.getTargetIndex() == null ? null : statement.getTargetIndex().copy();
    this.insertBody = statement.getInsertBody() == null ? null : statement.getInsertBody().copy();
    this.returnStatement = statement.getReturnStatement() == null ? null : statement.getReturnStatement().copy();
    this.selectStatement = statement.getSelectStatement() == null ? null : statement.getSelectStatement().copy();
  }

  public OInsertExecutionPlan createExecutionPlan(OCommandContext ctx) {
    OInsertExecutionPlan result = new OInsertExecutionPlan(ctx);

    if (targetIndex != null) {
      //TODO handle insert INSERT INTO INDEX SET key = foo, rid = #12:0
      throw new UnsupportedOperationException("Implement insert into index:");
    } else {
      if (selectStatement != null) {
        handleInsertSelect(result, this.selectStatement, ctx);
      } else {
        handleCreateRecord(result, this.insertBody, ctx);
      }
      handleTargetClass(result, targetClass, ctx);
      handleSetFields(result, insertBody, ctx);
      handleReturn(result, returnStatement, ctx);
      handleSave(result, targetClusterName, ctx);
    }
    return result;
  }

  private void handleSave(OInsertExecutionPlan result, OIdentifier targetClusterName, OCommandContext ctx) {
    result.chain(new SaveElementStep(ctx, targetClusterName));
  }

  private void handleReturn(OInsertExecutionPlan result, OProjection returnStatement, OCommandContext ctx) {
    if (returnStatement != null) {
      result.chain(new ProjectionCalculationStep(returnStatement, ctx));
    }
  }

  private void handleSetFields(OInsertExecutionPlan result, OInsertBody insertBody, OCommandContext ctx) {
    if (insertBody == null) {
      return;
    }
    if (insertBody.getIdentifierList() != null) {
      result.chain(new InsertValuesStep(insertBody.getIdentifierList(), insertBody.getValueExpressions(), ctx));
    } else if (insertBody.getContent() != null) {
      result.chain(new UpdateContentStep(insertBody.getContent(), ctx));
    } else if (insertBody.getSetExpressions() != null) {
      List<OUpdateItem> items = new ArrayList<>();
      for (OInsertSetExpression exp : insertBody.getSetExpressions()) {
        OUpdateItem item = new OUpdateItem(-1);
        item.setOperator(OUpdateItem.OPERATOR_EQ);
        item.setLeft(exp.getLeft().copy());
        item.setRight(exp.getRight().copy());
        items.add(item);
      }
      result.chain(new UpdateSetStep(items, ctx));
    }
  }

  private void handleTargetClass(OInsertExecutionPlan result, OIdentifier targetClass, OCommandContext ctx) {
    if (targetClass != null) {
      result.chain(new SetDocumentClassStep(targetClass, ctx));
    }
  }

  private void handleCreateRecord(OInsertExecutionPlan result, OInsertBody body, OCommandContext ctx) {
    int tot = 1;
    if (body.getValueExpressions() != null && body.getValueExpressions().size() > 0) {
      tot = body.getValueExpressions().size();
    }
    result.chain(new CreateRecordStep(ctx, tot));
  }

  private void handleInsertSelect(OInsertExecutionPlan result, OSelectStatement selectStatement, OCommandContext ctx) {
    OInternalExecutionPlan subPlan = selectStatement.createExecutionPlan(ctx);
    result.chain(new SubQueryStep(subPlan, ctx, ctx));
    result.chain(new CopyDocumentStep(result, ctx));
  }

}