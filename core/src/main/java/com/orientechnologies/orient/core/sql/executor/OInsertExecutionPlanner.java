package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.index.OIndexAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.sql.parser.OCluster;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import com.orientechnologies.orient.core.sql.parser.OIndexIdentifier;
import com.orientechnologies.orient.core.sql.parser.OInputParameter;
import com.orientechnologies.orient.core.sql.parser.OInsertBody;
import com.orientechnologies.orient.core.sql.parser.OInsertSetExpression;
import com.orientechnologies.orient.core.sql.parser.OInsertStatement;
import com.orientechnologies.orient.core.sql.parser.OJson;
import com.orientechnologies.orient.core.sql.parser.OProjection;
import com.orientechnologies.orient.core.sql.parser.OSelectStatement;
import com.orientechnologies.orient.core.sql.parser.OUpdateItem;
import java.util.ArrayList;
import java.util.List;

/** Created by luigidellaquila on 08/08/16. */
public class OInsertExecutionPlanner {

  protected OIdentifier targetClass;
  protected OIdentifier targetClusterName;
  protected OCluster targetCluster;
  protected OIndexIdentifier targetIndex;
  protected OInsertBody insertBody;
  protected OProjection returnStatement;
  protected OSelectStatement selectStatement;

  public OInsertExecutionPlanner() {}

  public OInsertExecutionPlanner(OInsertStatement statement) {
    this.targetClass =
        statement.getTargetClass() == null ? null : statement.getTargetClass().copy();
    this.targetClusterName =
        statement.getTargetClusterName() == null ? null : statement.getTargetClusterName().copy();
    this.targetCluster =
        statement.getTargetCluster() == null ? null : statement.getTargetCluster().copy();
    this.targetIndex =
        statement.getTargetIndex() == null ? null : statement.getTargetIndex().copy();
    this.insertBody = statement.getInsertBody() == null ? null : statement.getInsertBody().copy();
    this.returnStatement =
        statement.getReturnStatement() == null ? null : statement.getReturnStatement().copy();
    this.selectStatement =
        statement.getSelectStatement() == null ? null : statement.getSelectStatement().copy();
  }

  public OInsertExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {
    OInsertExecutionPlan result = new OInsertExecutionPlan(ctx);

    if (targetIndex != null) {
      OIndexAbstract.manualIndexesWarning();
      result.chain(new InsertIntoIndexStep(targetIndex, insertBody, ctx, enableProfiling));
    } else {
      if (selectStatement != null) {
        handleInsertSelect(result, this.selectStatement, ctx, enableProfiling);
      } else {
        handleCreateRecord(result, this.insertBody, ctx, enableProfiling);
      }
      handleTargetClass(result, ctx, enableProfiling);
      handleSetFields(result, insertBody, ctx, enableProfiling);
      ODatabaseSession database = ctx.getDatabase();
      if (targetCluster != null) {
        String name = targetCluster.getClusterName();
        if (name == null) {
          name = database.getClusterNameById(targetCluster.getClusterNumber());
        }
        handleSave(result, new OIdentifier(name), ctx, enableProfiling);
      } else {
        handleSave(result, targetClusterName, ctx, enableProfiling);
      }
      handleReturn(result, returnStatement, ctx, enableProfiling);
    }
    return result;
  }

  private void handleSave(
      OInsertExecutionPlan result,
      OIdentifier targetClusterName,
      OCommandContext ctx,
      boolean profilingEnabled) {
    result.chain(new SaveElementStep(ctx, targetClusterName, profilingEnabled));
  }

  private void handleReturn(
      OInsertExecutionPlan result,
      OProjection returnStatement,
      OCommandContext ctx,
      boolean profilingEnabled) {
    if (returnStatement != null) {
      result.chain(new ProjectionCalculationStep(returnStatement, ctx, profilingEnabled));
    }
  }

  private void handleSetFields(
      OInsertExecutionPlan result,
      OInsertBody insertBody,
      OCommandContext ctx,
      boolean profilingEnabled) {
    if (insertBody == null) {
      return;
    }
    if (insertBody.getIdentifierList() != null) {
      result.chain(
          new InsertValuesStep(
              insertBody.getIdentifierList(),
              insertBody.getValueExpressions(),
              ctx,
              profilingEnabled));
    } else if (insertBody.getContent() != null) {
      for (OJson json : insertBody.getContent()) {
        result.chain(new UpdateContentStep(json, ctx, profilingEnabled));
      }
    } else if (insertBody.getContentInputParam() != null) {
      for (OInputParameter inputParam : insertBody.getContentInputParam()) {
        result.chain(new UpdateContentStep(inputParam, ctx, profilingEnabled));
      }
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

  private void handleTargetClass(
      OInsertExecutionPlan result, OCommandContext ctx, boolean profilingEnabled) {
    ODatabaseSession database = ctx.getDatabase();
    OSchema schema = database.getMetadata().getSchema();
    OIdentifier tc = null;
    if (targetClass != null) {
      tc = targetClass;
    } else if (targetCluster != null) {
      String name = targetCluster.getClusterName();
      if (name == null) {
        name = database.getClusterNameById(targetCluster.getClusterNumber());
      }
      OClass targetClass = schema.getClassByClusterId(database.getClusterIdByName(name));
      if (targetClass != null) {
        tc = new OIdentifier(targetClass.getName());
      }
    } else if (this.targetClass == null) {

      OClass targetClass =
          schema.getClassByClusterId(
              database.getClusterIdByName(targetClusterName.getStringValue()));
      if (targetClass != null) {
        tc = new OIdentifier(targetClass.getName());
      }
    }
    if (tc != null) {
      result.chain(new SetDocumentClassStep(tc, ctx, profilingEnabled));
    }
  }

  private void handleCreateRecord(
      OInsertExecutionPlan result,
      OInsertBody body,
      OCommandContext ctx,
      boolean profilingEnabled) {
    int tot = 1;

    if (body != null
        && body.getValueExpressions() != null
        && body.getValueExpressions().size() > 0) {
      tot = body.getValueExpressions().size();
    }
    if (body != null
        && body.getContentInputParam() != null
        && body.getContentInputParam().size() > 0) {
      tot = body.getContentInputParam().size();
      if (body != null && body.getContent() != null && body.getContent().size() > 0) {
        tot += body.getContent().size();
      }
    } else {
      if (body != null && body.getContent() != null && body.getContent().size() > 0) {
        tot = body.getContent().size();
      }
    }
    result.chain(new CreateRecordStep(ctx, tot, profilingEnabled));
  }

  private void handleInsertSelect(
      OInsertExecutionPlan result,
      OSelectStatement selectStatement,
      OCommandContext ctx,
      boolean profilingEnabled) {
    OInternalExecutionPlan subPlan = selectStatement.createExecutionPlan(ctx, profilingEnabled);
    result.chain(new SubQueryStep(subPlan, ctx, ctx, profilingEnabled));
    result.chain(new CopyDocumentStep(ctx, profilingEnabled));
    result.chain(new RemoveEdgePointersStep(ctx, profilingEnabled));
  }
}
