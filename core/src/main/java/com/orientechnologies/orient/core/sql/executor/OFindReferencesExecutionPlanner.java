package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OFindReferencesExecutionPlanner {

  protected ORid       rid;
  protected OStatement subQuery;

  //class or cluster
  protected List<SimpleNode> targets;

  public OFindReferencesExecutionPlanner(OFindReferencesStatement statement) {
    //copying the content, so that it can be manipulated and optimized
    this.rid = statement.getRid() == null ? null : statement.getRid().copy();
    this.subQuery = statement.getSubQuery() == null ? null : statement.getSubQuery().copy();
    this.targets =
        statement.getTargets() == null ? null : statement.getTargets().stream().map(x -> x.copy()).collect(Collectors.toList());
  }

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx) {
    OSelectExecutionPlan plan = new OSelectExecutionPlan(ctx);
    handleRidSource(plan, ctx);
    handleSubQuerySource(plan, ctx);
    handleFindReferences(plan, ctx);
    return plan;
  }

  private void handleFindReferences(OSelectExecutionPlan plan, OCommandContext ctx) {
    List<OIdentifier> classes = targets == null ?
        null :
        targets.stream().filter(x -> x instanceof OIdentifier).map(y -> ((OIdentifier) y)).collect(Collectors.toList());

    List<OCluster> clusters = targets == null ?
        null :
        targets.stream().filter(x -> x instanceof OCluster).map(y -> ((OCluster) y)).collect(Collectors.toList());

    plan.chain(new FindReferencesStep(classes, clusters, ctx));
  }

  private void handleSubQuerySource(OSelectExecutionPlan plan, OCommandContext ctx) {
    if (subQuery != null) {
      new SubQueryStep(subQuery.createExecutionPlan(ctx), ctx, ctx);
    }
  }

  private void handleRidSource(OSelectExecutionPlan plan, OCommandContext ctx) {
    if (rid != null) {
      plan.chain(new FetchFromRidsStep(Collections.singleton(rid.toRecordId()), ctx));
    }
  }

}
