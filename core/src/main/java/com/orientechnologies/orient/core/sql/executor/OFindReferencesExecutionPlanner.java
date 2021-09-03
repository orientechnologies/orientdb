package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OCluster;
import com.orientechnologies.orient.core.sql.parser.OFindReferencesStatement;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import com.orientechnologies.orient.core.sql.parser.ORid;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.sql.parser.SimpleNode;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OFindReferencesExecutionPlanner {

  protected ORid rid;
  protected OStatement subQuery;

  // class or cluster
  protected List<SimpleNode> targets;

  public OFindReferencesExecutionPlanner(OFindReferencesStatement statement) {
    // copying the content, so that it can be manipulated and optimized
    this.rid = statement.getRid() == null ? null : statement.getRid().copy();
    this.subQuery = statement.getSubQuery() == null ? null : statement.getSubQuery().copy();
    this.targets =
        statement.getTargets() == null
            ? null
            : statement.getTargets().stream().map(x -> x.copy()).collect(Collectors.toList());
  }

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {
    OSelectExecutionPlan plan = new OSelectExecutionPlan(ctx);
    handleRidSource(plan, ctx, enableProfiling);
    handleSubQuerySource(plan, ctx, enableProfiling);
    handleFindReferences(plan, ctx, enableProfiling);
    return plan;
  }

  private void handleFindReferences(
      OSelectExecutionPlan plan, OCommandContext ctx, boolean profilingEnabled) {
    List<OIdentifier> classes = null;
    List<OCluster> clusters = null;
    if (targets != null) {
      classes =
          targets.stream()
              .filter(x -> x instanceof OIdentifier)
              .map(y -> ((OIdentifier) y))
              .collect(Collectors.toList());
      clusters =
          targets.stream()
              .filter(x -> x instanceof OCluster)
              .map(y -> ((OCluster) y))
              .collect(Collectors.toList());
    }

    plan.chain(new FindReferencesStep(classes, clusters, ctx, profilingEnabled));
  }

  private void handleSubQuerySource(
      OSelectExecutionPlan plan, OCommandContext ctx, boolean profilingEnabled) {
    if (subQuery != null) {
      plan.chain(
          new SubQueryStep(
              subQuery.createExecutionPlan(ctx, profilingEnabled), ctx, ctx, profilingEnabled));
    }
  }

  private void handleRidSource(
      OSelectExecutionPlan plan, OCommandContext ctx, boolean profilingEnabled) {
    if (rid != null) {
      plan.chain(
          new FetchFromRidsStep(
              Collections.singleton(rid.toRecordId((OResult) null, ctx)), ctx, profilingEnabled));
    }
  }
}
