package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexCandidateOne;
import com.orientechnologies.orient.core.sql.parser.OCluster;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
import com.orientechnologies.orient.core.sql.parser.OFromItem;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import com.orientechnologies.orient.core.sql.parser.OIndexIdentifier;
import com.orientechnologies.orient.core.sql.parser.OInputParameter;
import com.orientechnologies.orient.core.sql.parser.OInteger;
import com.orientechnologies.orient.core.sql.parser.OLimit;
import com.orientechnologies.orient.core.sql.parser.OMetadataIdentifier;
import com.orientechnologies.orient.core.sql.parser.ORid;
import com.orientechnologies.orient.core.sql.parser.OSkip;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.sql.parser.OTraverseProjectionItem;
import com.orientechnologies.orient.core.sql.parser.OTraverseStatement;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OTraverseExecutionPlanner {

  private List<OTraverseProjectionItem> projections = null;
  private OFromClause target;

  private OWhereClause whileClause;

  private final OTraverseStatement.Strategy strategy;
  private final OInteger maxDepth;

  private OSkip skip;
  private OLimit limit;

  public OTraverseExecutionPlanner(OTraverseStatement statement) {
    // copying the content, so that it can be manipulated and optimized
    if (statement.getProjections() == null) {
      this.projections = null;
    } else {
      this.projections =
          statement.getProjections().stream().map(x -> x.copy()).collect(Collectors.toList());
    }

    this.target = statement.getTarget();
    this.whileClause =
        statement.getWhileClause() == null ? null : statement.getWhileClause().copy();

    this.strategy =
        statement.getStrategy() == null
            ? OTraverseStatement.Strategy.DEPTH_FIRST
            : statement.getStrategy();
    this.maxDepth = statement.getMaxDepth() == null ? null : statement.getMaxDepth().copy();

    this.skip = statement.getSkip();
    this.limit = statement.getLimit();
  }

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx) {
    OSelectExecutionPlan result = new OSelectExecutionPlan();

    handleFetchFromTarger(result, ctx);

    handleTraversal(result);

    if (skip != null) {
      result.chain(new SkipExecutionStep(skip));
    }
    if (limit != null) {
      result.chain(new LimitExecutionStep(limit));
    }

    return result;
  }

  private void handleTraversal(OSelectExecutionPlan result) {
    switch (strategy) {
      case BREADTH_FIRST:
        result.chain(new BreadthFirstTraverseStep(this.projections, this.whileClause, maxDepth));
        break;
      case DEPTH_FIRST:
        result.chain(new DepthFirstTraverseStep(this.projections, this.whileClause, maxDepth));
        break;
    }
    // TODO
  }

  private void handleFetchFromTarger(OSelectExecutionPlan result, OCommandContext ctx) {

    OFromItem target = this.target == null ? null : this.target.getItem();
    if (target == null) {
      handleNoTarget(result);
    } else if (target.getIdentifier() != null) {
      String className = target.getIdentifier().getStringValue();
      if (className.startsWith("$")
          && !((ODatabaseDocumentInternal) ctx.getDatabase())
              .getMetadata()
              .getImmutableSchemaSnapshot()
              .existsClass(className)) {
        handleVariableAsTarget(result, this.target);
      } else {
        handleClassAsTarget(result, this.target, ctx);
      }
    } else if (target.getCluster() != null) {
      handleClustersAsTarget(result, Collections.singletonList(target.getCluster()), ctx);
    } else if (target.getClusterList() != null) {
      handleClustersAsTarget(result, target.getClusterList().toListOfClusters(), ctx);
    } else if (target.getStatement() != null) {
      handleSubqueryAsTarget(result, target.getStatement(), ctx);
    } else if (target.getFunctionCall() != null) {
      //        handleFunctionCallAsTarget(result, target.getFunctionCall(), ctx);//TODO
      throw new OCommandExecutionException("function call as target is not supported yet");
    } else if (target.getInputParam() != null) {
      handleInputParamAsTarget(result, target.getInputParam(), ctx);
    } else if (target.getIndex() != null) {
      handleIndexAsTarget(result, target.getIndex(), ctx);
    } else if (target.getMetadata() != null) {
      handleMetadataAsTarget(result, target.getMetadata(), ctx);
    } else if (target.getRids() != null && target.getRids().size() > 0) {
      handleRidsAsTarget(result, target.getRids(), ctx);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private void handleInputParamAsTarget(
      OSelectExecutionPlan result, OInputParameter inputParam, OCommandContext ctx) {
    Object paramValue = inputParam.getValue(ctx.getInputParameters());
    if (paramValue == null) {
      result.chain(new EmptyStep()); // nothing to return
    } else if (paramValue instanceof OClass) {
      OFromClause from = new OFromClause(-1);
      OFromItem item = new OFromItem(-1);
      from.setItem(item);
      item.setIdentifier(new OIdentifier(((OClass) paramValue).getName()));
      handleClassAsTarget(result, from, ctx);
    } else if (paramValue instanceof String) {
      // strings are treated as classes
      OFromClause from = new OFromClause(-1);
      OFromItem item = new OFromItem(-1);
      from.setItem(item);
      item.setIdentifier(new OIdentifier((String) paramValue));
      handleClassAsTarget(result, from, ctx);
    } else if (paramValue instanceof OIdentifiable) {
      ORID orid = ((OIdentifiable) paramValue).getIdentity();

      ORid rid = new ORid(-1);
      OInteger cluster = new OInteger(-1);
      cluster.setValue(orid.getClusterId());
      OInteger position = new OInteger(-1);
      position.setValue(orid.getClusterPosition());
      rid.setLegacy(true);
      rid.setCluster(cluster);
      rid.setPosition(position);

      handleRidsAsTarget(result, Collections.singletonList(rid), ctx);
    } else if (paramValue instanceof Iterable) {
      // try list of RIDs
      List<ORid> rids = new ArrayList<>();
      for (Object x : (Iterable) paramValue) {
        if (!(x instanceof OIdentifiable)) {
          throw new OCommandExecutionException("Cannot use colleciton as target: " + paramValue);
        }
        ORID orid = ((OIdentifiable) x).getIdentity();

        ORid rid = new ORid(-1);
        OInteger cluster = new OInteger(-1);
        cluster.setValue(orid.getClusterId());
        OInteger position = new OInteger(-1);
        position.setValue(orid.getClusterPosition());
        rid.setCluster(cluster);
        rid.setPosition(position);

        rids.add(rid);
      }
      handleRidsAsTarget(result, rids, ctx);
    } else {
      throw new OCommandExecutionException("Invalid target: " + paramValue);
    }
  }

  private void handleNoTarget(OSelectExecutionPlan result) {
    result.chain(new EmptyDataGeneratorStep(1));
  }

  private void handleIndexAsTarget(
      OSelectExecutionPlan result, OIndexIdentifier indexIdentifier, OCommandContext ctx) {
    String indexName = indexIdentifier.getIndexName();
    final ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
    OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, indexName);
    if (index == null) {
      throw new OCommandExecutionException("Index not found: " + indexName);
    }

    switch (indexIdentifier.getType()) {
      case INDEX:
        if (!index.supportsOrderedIterations()) {
          throw new OCommandExecutionException(
              "Index " + indexName + " does not allow iteration without a condition");
        }

        result.chain(new FetchFromIndexStep(new OIndexCandidateOne(index.getName()), true, ctx));
        result.chain(new GetValueFromIndexEntryStep(null));
        break;
      case VALUES:
      case VALUESASC:
        if (!index.supportsOrderedIterations()) {
          throw new OCommandExecutionException(
              "Index " + indexName + " does not allow iteration on values");
        }
        result.chain(
            new FetchFromIndexValuesStep(new OIndexCandidateOne(index.getName()), true, ctx));
        result.chain(new GetValueFromIndexEntryStep(null));
        break;
      case VALUESDESC:
        if (!index.supportsOrderedIterations()) {
          throw new OCommandExecutionException(
              "Index " + indexName + " does not allow iteration on values");
        }
        result.chain(
            new FetchFromIndexValuesStep(new OIndexCandidateOne(index.getName()), false, ctx));
        result.chain(new GetValueFromIndexEntryStep(null));
        break;
    }
  }

  private void handleMetadataAsTarget(
      OSelectExecutionPlan plan, OMetadataIdentifier metadata, OCommandContext ctx) {
    ODatabaseInternal db = (ODatabaseInternal) ctx.getDatabase();
    String schemaRecordIdAsString = null;
    if (metadata.getName().equalsIgnoreCase("schema")) {
      schemaRecordIdAsString = db.getStorageInfo().getConfiguration().getSchemaRecordId();
    } else if (metadata.getName().equalsIgnoreCase("indexmanager")) {
      schemaRecordIdAsString = db.getStorageInfo().getConfiguration().getIndexMgrRecordId();
    } else {
      throw new UnsupportedOperationException("Invalid metadata: " + metadata.getName());
    }
    ORecordId schemaRid = new ORecordId(schemaRecordIdAsString);
    plan.chain(new FetchFromRidsStep(Collections.singleton(schemaRid)));
  }

  private void handleRidsAsTarget(OSelectExecutionPlan plan, List<ORid> rids, OCommandContext ctx) {
    List<ORecordId> actualRids = new ArrayList<>();
    for (ORid rid : rids) {
      actualRids.add(rid.toRecordId((OResult) null, ctx));
    }
    plan.chain(new FetchFromRidsStep(actualRids));
  }

  private void handleClassAsTarget(
      OSelectExecutionPlan plan, OFromClause queryTarget, OCommandContext ctx) {
    OIdentifier identifier = queryTarget.getItem().getIdentifier();

    Boolean orderByRidAsc = null; // null: no order. true: asc, false:desc
    FetchFromClassExecutionStep fetcher =
        new FetchFromClassExecutionStep(identifier.getStringValue(), null, ctx, orderByRidAsc);
    plan.chain(fetcher);
  }

  private void handleClustersAsTarget(
      OSelectExecutionPlan plan, List<OCluster> clusters, OCommandContext ctx) {
    ODatabaseSession db = ctx.getDatabase();
    Boolean orderByRidAsc = null; // null: no order. true: asc, false:desc
    if (clusters.size() == 1) {
      OCluster cluster = clusters.get(0);
      Integer clusterId = cluster.getClusterNumber();
      if (clusterId == null) {
        clusterId = db.getClusterIdByName(cluster.getClusterName());
      }
      if (clusterId == -1) {
        throw new OCommandExecutionException("Cluster " + cluster + " does not exist");
      }
      FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(clusterId);
      if (Boolean.TRUE.equals(orderByRidAsc)) {
        step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
      } else if (Boolean.FALSE.equals(orderByRidAsc)) {
        step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);
      }
      plan.chain(step);
    } else {
      int[] clusterIds = new int[clusters.size()];
      for (int i = 0; i < clusters.size(); i++) {
        OCluster cluster = clusters.get(i);
        Integer clusterId = cluster.getClusterNumber();
        if (clusterId == null) {
          clusterId = db.getClusterIdByName(cluster.getClusterName());
        }
        if (clusterId == -1) {
          throw new OCommandExecutionException("Cluster " + cluster + " does not exist");
        }
        clusterIds[i] = clusterId;
      }
      FetchFromClustersExecutionStep step =
          new FetchFromClustersExecutionStep(clusterIds, orderByRidAsc);
      plan.chain(step);
    }
  }

  private void handleSubqueryAsTarget(
      OSelectExecutionPlan plan, OStatement subQuery, OCommandContext ctx) {
    OBasicCommandContext subCtx = new OBasicCommandContext(ctx.getDatabase());
    subCtx.setParent(ctx);
    OInternalExecutionPlan subExecutionPlan = subQuery.createExecutionPlan(subCtx);
    plan.chain(new SubQueryStep(subExecutionPlan, ctx, subCtx));
  }

  private void handleVariableAsTarget(OSelectExecutionPlan plan, OFromClause target) {
    plan.chain(new FetchFromVariableStep(target.getItem()));
  }
}
