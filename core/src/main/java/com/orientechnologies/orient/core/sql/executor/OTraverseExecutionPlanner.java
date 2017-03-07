package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OTraverseExecutionPlanner {

  private List<OTraverseProjectionItem> projections = null;
  private OFromClause target;

  private OWhereClause whileClause;

  private final OTraverseStatement.Strategy strategy;
  private final OInteger                    maxDepth;

  private OSkip  skip;
  private OLimit limit;

  public OTraverseExecutionPlanner(OTraverseStatement statement) {
    //copying the content, so that it can be manipulated and optimized
    this.projections = statement.getProjections() == null ?
        null :
        statement.getProjections().stream().map(x -> x.copy()).collect(Collectors.toList());

    this.target = statement.getTarget();
    this.whileClause = statement.getWhileClause() == null ? null : statement.getWhileClause().copy();

    this.strategy = statement.getStrategy() == null ? OTraverseStatement.Strategy.DEPTH_FIRST : statement.getStrategy();
    this.maxDepth = statement.getMaxDepth() == null ? null : statement.getMaxDepth().copy();

    this.skip = statement.getSkip();
    this.limit = statement.getLimit();
  }

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx) {
    OSelectExecutionPlan result = new OSelectExecutionPlan(ctx);

    handleFetchFromTarger(result, ctx);

    handleTraversal(result, ctx);

    if (skip != null) {
      result.chain(new SkipExecutionStep(skip, ctx));
    }
    if (limit != null) {
      result.chain(new LimitExecutionStep(limit, ctx));
    }

    return result;
  }

  private void handleTraversal(OSelectExecutionPlan result, OCommandContext ctx) {
    switch (strategy) {
    case BREADTH_FIRST:
      result.chain(new BreadthFirstTraverseStep(this.projections, this.whileClause, ctx));
      break;
    case DEPTH_FIRST:
      result.chain(new DepthFirstTraverseStep(this.projections, this.whileClause, ctx));
      break;
    }
    //TODO
  }

  private void handleFetchFromTarger(OSelectExecutionPlan result, OCommandContext ctx) {

    OFromItem target = this.target == null ? null : this.target.getItem();
    if (target == null) {
      handleNoTarget(result, ctx);
    } else if (target.getIdentifier() != null) {
      handleClassAsTarget(result, this.target, ctx);
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

  private void handleInputParamAsTarget(OSelectExecutionPlan result, OInputParameter inputParam, OCommandContext ctx) {
    Object paramValue = inputParam.getValue(ctx.getInputParameters());
    if (paramValue == null) {
      result.chain(new EmptyStep(ctx));//nothing to return
    } else if (paramValue instanceof OClass) {
      OFromClause from = new OFromClause(-1);
      OFromItem item = new OFromItem(-1);
      from.setItem(item);
      item.setIdentifier(new OIdentifier(((OClass) paramValue).getName()));
      handleClassAsTarget(result, from, ctx);
    } else if (paramValue instanceof String) {
      //strings are treated as classes
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
      rid.setCluster(cluster);
      rid.setPosition(position);

      handleRidsAsTarget(result, Collections.singletonList(rid), ctx);
    } else if (paramValue instanceof Iterable) {
      //try list of RIDs
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

  private void handleNoTarget(OSelectExecutionPlan result, OCommandContext ctx) {
    result.chain(new EmptyDataGeneratorStep(1, ctx));
  }

  private void handleIndexAsTarget(OSelectExecutionPlan result, OIndexIdentifier indexIdentifier, OCommandContext ctx) {
    String indexName = indexIdentifier.getIndexName();
    OIndex<?> index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(indexName);
    if (index == null) {
      throw new OCommandExecutionException("Index not found: " + indexName);
    }

    switch (indexIdentifier.getType()) {
    case INDEX:

      if (!index.supportsOrderedIterations()) {
        throw new OCommandExecutionException("Index " + indexName + " does not allow iteration without a condition");
      }

      result.chain(new FetchFromIndexStep(index, null, null, ctx));
      break;
    case VALUES:
    case VALUESASC:
      if (!index.supportsOrderedIterations()) {
        throw new OCommandExecutionException("Index " + indexName + " does not allow iteration on values");
      }
      result.chain(new FetchFromIndexValuesStep(index, true, ctx));
      break;
    case VALUESDESC:
      if (!index.supportsOrderedIterations()) {
        throw new OCommandExecutionException("Index " + indexName + " does not allow iteration on values");
      }
      result.chain(new FetchFromIndexValuesStep(index, false, ctx));
      break;
    }
  }

  private void handleMetadataAsTarget(OSelectExecutionPlan plan, OMetadataIdentifier metadata, OCommandContext ctx) {
    ODatabaseInternal db = (ODatabaseInternal) ctx.getDatabase();
    String schemaRecordIdAsString = null;
    if (metadata.getName().equalsIgnoreCase(OCommandExecutorSQLAbstract.METADATA_SCHEMA)) {
      schemaRecordIdAsString = db.getStorage().getConfiguration().schemaRecordId;
    } else if (metadata.getName().equalsIgnoreCase(OCommandExecutorSQLAbstract.METADATA_INDEXMGR)) {
      schemaRecordIdAsString = db.getStorage().getConfiguration().indexMgrRecordId;
    } else {
      throw new UnsupportedOperationException("Invalid metadata: " + metadata.getName());
    }
    ORecordId schemaRid = new ORecordId(schemaRecordIdAsString);
    plan.chain(new FetchFromRidsStep(Collections.singleton(schemaRid), ctx));

  }

  private void handleRidsAsTarget(OSelectExecutionPlan plan, List<ORid> rids, OCommandContext ctx) {
    List<ORecordId> actualRids = new ArrayList<>();
    for (ORid rid : rids) {
      actualRids.add(rid.toRecordId());
    }
    plan.chain(new FetchFromRidsStep(actualRids, ctx));
  }

  private void handleClassAsTarget(OSelectExecutionPlan plan, OFromClause queryTarget, OCommandContext ctx) {
    OIdentifier identifier = queryTarget.getItem().getIdentifier();

    Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    FetchFromClassExecutionStep fetcher = new FetchFromClassExecutionStep(identifier.getStringValue(), ctx, orderByRidAsc);
    plan.chain(fetcher);
  }

  private void handleClustersAsTarget(OSelectExecutionPlan plan, List<OCluster> clusters, OCommandContext ctx) {
    ODatabase db = ctx.getDatabase();
    Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    if (clusters.size() == 1) {
      OCluster cluster = clusters.get(0);
      Integer clusterId = cluster.getClusterNumber();
      if (clusterId == null) {
        clusterId = db.getClusterIdByName(cluster.getClusterName());
      }
      if (clusterId == null) {
        throw new OCommandExecutionException("Cluster " + cluster + " does not exist");
      }
      FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(clusterId, ctx);
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
        if (clusterId == null) {
          throw new OCommandExecutionException("Cluster " + cluster + " does not exist");
        }
        clusterIds[i] = clusterId;
      }
      FetchFromClustersExecutionStep step = new FetchFromClustersExecutionStep(clusterIds, ctx, orderByRidAsc);
      plan.chain(step);
    }
  }

  private void handleSubqueryAsTarget(OSelectExecutionPlan plan, OStatement subQuery, OCommandContext ctx) {
    OBasicCommandContext subCtx = new OBasicCommandContext();
    subCtx.setDatabase(ctx.getDatabase());
    subCtx.setParent(ctx);
    OInternalExecutionPlan subExecutionPlan = subQuery.createExecutionPlan(subCtx);
    plan.chain(new SubQueryStep(subExecutionPlan, ctx, subCtx));
  }

}
