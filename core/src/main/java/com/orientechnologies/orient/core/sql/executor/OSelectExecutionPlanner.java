package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OSelectExecutionPlanner {

  QueryPlanningInfo info;

  public OSelectExecutionPlanner(OSelectStatement oSelectStatement) {
    //copying the content, so that it can be manipulated and optimized
    info = new QueryPlanningInfo();
    info.projection = oSelectStatement.getProjection() == null ? null : oSelectStatement.getProjection().copy();
    info.projection = translateDistinct(info.projection);
    info.distinct = info.projection == null ? false : info.projection.isDistinct();
    if (info.projection != null) {
      info.projection.setDistinct(false);
    }

    info.target = oSelectStatement.getTarget();
    info.whereClause = oSelectStatement.getWhereClause() == null ? null : oSelectStatement.getWhereClause().copy();
    info.whereClause = translateLucene(info.whereClause);
    info.perRecordLetClause = oSelectStatement.getLetClause() == null ? null : oSelectStatement.getLetClause().copy();
    info.groupBy = oSelectStatement.getGroupBy() == null ? null : oSelectStatement.getGroupBy().copy();
    info.orderBy = oSelectStatement.getOrderBy() == null ? null : oSelectStatement.getOrderBy().copy();
    info.unwind = oSelectStatement.getUnwind() == null ? null : oSelectStatement.getUnwind().copy();
    info.skip = oSelectStatement.getSkip();
    info.limit = oSelectStatement.getLimit();
  }

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {
    OSelectExecutionPlan result = new OSelectExecutionPlan(ctx);

    if (info.expand && info.distinct) {
      throw new OCommandExecutionException("Cannot execute a statement with DISTINCT expand(), please use a subquery");
    }

    optimizeQuery(info);

    if (handleHardwiredOptimizations(result, ctx, enableProfiling)) {
      return result;
    }

    handleGlobalLet(result, info, ctx, enableProfiling);

    calculateShardingStrategy(info, ctx);

    handleFetchFromTarger(result, info, ctx, enableProfiling);

    handleLet(result, info, ctx, enableProfiling);

    handleWhere(result, info, ctx, enableProfiling);

    handleProjectionsBeforeOrderBy(result, info, ctx, enableProfiling);
    if (info.expand || info.unwind != null) {

      handleProjections(result, info, ctx, enableProfiling);
      handleExpand(result, info, ctx, enableProfiling);
      handleUnwind(result, info, ctx, enableProfiling);
      handleOrderBy(result, info, ctx, enableProfiling);
      if (info.skip != null) {
        result.chain(new SkipExecutionStep(info.skip, ctx, enableProfiling));
      }
      if (info.limit != null) {
        result.chain(new LimitExecutionStep(info.limit, ctx, enableProfiling));
      }
    } else {
      handleOrderBy(result, info, ctx, enableProfiling);
      if (info.distinct) {
        handleProjections(result, info, ctx, enableProfiling);
        handleDistinct(result, info, ctx, enableProfiling);
        if (info.skip != null) {
          result.chain(new SkipExecutionStep(info.skip, ctx, enableProfiling));
        }
        if (info.limit != null) {
          result.chain(new LimitExecutionStep(info.limit, ctx, enableProfiling));
        }
      } else {
        if (info.skip != null) {
          result.chain(new SkipExecutionStep(info.skip, ctx, enableProfiling));
        }
        if (info.limit != null) {
          result.chain(new LimitExecutionStep(info.limit, ctx, enableProfiling));
        }
        handleProjections(result, info, ctx, enableProfiling);
      }
    }
    return result;
  }

  /**
   * based on the cluster/server map and the query target, this method tries to find an optimal
   * strategy to execute the query on the cluster.
   *
   * @param info
   * @param ctx
   */
  private void calculateShardingStrategy(QueryPlanningInfo info, OCommandContext ctx) {
    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) ctx.getDatabase();

    Map<String, Set<String>> clusterMap = db.getActiveClusterMap();
    Set<String> queryClusters = calculateTargetClusters(info, ctx);
    if (queryClusters == null) {
      return;
    }
//    Set<String> serversWithAllTheClusers = getServersThatHasAllClusters(clusterMap, queryClusters);
//    if (serversWithAllTheClusers.isEmpty()) {
      // sharded query
      Map<String, Set<String>> minimalSetOfNodes = getMinimalSetOfNodesForShardedQuery(db.getLocalNodeName(), clusterMap,
          queryClusters);
      if (minimalSetOfNodes == null) {
        throw new OCommandExecutionException("Cannot execute sharded query");
      }
      info.serverToClusters = minimalSetOfNodes;
//    } else {
//      // all on a node
//      String targetNode = serversWithAllTheClusers.contains(db.getLocalNodeName()) ?
//          db.getLocalNodeName() :
//          serversWithAllTheClusers.iterator().next();
//      info.serverToClusters = new HashMap<>();
//      info.serverToClusters.put(targetNode, queryClusters);
//    }
  }

  /**
   * given a cluster map and a set of clusters involved in a query, tries to calculate the minimum number of nodes that
   * will have to be involved in the query execution, with clusters involved for each node.
   *
   * @param clusterMap
   * @param queryClusters
   *
   * @return a map that has node names as a key and clusters (data files) for each node as a value
   */
  private Map<String, Set<String>> getMinimalSetOfNodesForShardedQuery(String localNode, Map<String, Set<String>> clusterMap,
      Set<String> queryClusters) {
    //approximate algorithm, the problem is NP-complete
    Map<String, Set<String>> result = new HashMap<>();
    Set<String> uncovered = new HashSet<>();
    uncovered.addAll(queryClusters);

    //try local node first
    Set<String> nextNodeClusters = new HashSet<>();
    nextNodeClusters.addAll(clusterMap.get(localNode));
    nextNodeClusters.retainAll(uncovered);
    if (nextNodeClusters.size() > 0) {
      result.put(localNode, nextNodeClusters);
      uncovered.removeAll(nextNodeClusters);
    }

    while (uncovered.size() > 0) {
      String nextNode = findItemThatCoversMore(uncovered, clusterMap);
      nextNodeClusters = new HashSet<>();
      nextNodeClusters.addAll(clusterMap.get(nextNode));
      nextNodeClusters.retainAll(uncovered);
      result.put(nextNode, nextNodeClusters);
      uncovered.removeAll(nextNodeClusters);
    }
    return result;
  }

  private String findItemThatCoversMore(Set<String> uncovered, Map<String, Set<String>> clusterMap) {
    String lastFound = null;
    int lastSize = -1;
    for (Map.Entry<String, Set<String>> nodeConfig : clusterMap.entrySet()) {
      Set<String> current = new HashSet<>();
      current.addAll(nodeConfig.getValue());
      current.retainAll(uncovered);
      int thisSize = current.size();
      if (lastFound == null || thisSize > lastSize) {
        lastFound = nodeConfig.getKey();
        lastSize = thisSize;
      }
    }
    return lastFound;

  }

  /**
   * @param clusterMap    the cluster map for current sharding configuration
   * @param queryClusters the clusters that are target of the query
   *
   * @return
   */
  private Set<String> getServersThatHasAllClusters(Map<String, Set<String>> clusterMap, Set<String> queryClusters) {
    Set<String> remainingServers = clusterMap.keySet();
    for (String cluster : queryClusters) {
      for (Map.Entry<String, Set<String>> serverConfig : clusterMap.entrySet()) {
        if (!serverConfig.getValue().contains(cluster)) {
          remainingServers.remove(serverConfig.getKey());
        }
      }
    }
    return remainingServers;
  }

  /**
   * tries to calculate which clusters will be impacted by this query
   *
   * @param info
   * @param ctx
   *
   * @return a set of cluster names this query will fetch from
   */
  private Set<String> calculateTargetClusters(QueryPlanningInfo info, OCommandContext ctx) {
    if (info.target == null) {
      return Collections.EMPTY_SET;
    }

    Set<String> result = new HashSet<>();
    ODatabase db = ctx.getDatabase();
    OFromItem item = info.target.getItem();
    if (item.getRids() != null && item.getRids().size() > 0) {
      if (item.getRids().size() == 1) {
        OInteger cluster = item.getRids().get(0).getCluster();
        result.add(db.getClusterNameById(cluster.getValue().intValue()));
      } else {
        for (ORid rid : item.getRids()) {
          OInteger cluster = rid.getCluster();
          result.add(db.getClusterNameById(cluster.getValue().intValue()));
        }
      }
      return result;
    } else if (item.getInputParams() != null && item.getInputParams().size() > 0) {
      if (((ODatabaseInternal) ctx.getDatabase()).isSharded()) {
        throw new UnsupportedOperationException("Sharded query with input parameter as a target is not supported yet");
      }
      return null;
    } else if (item.getCluster() != null) {
      String name = item.getCluster().getClusterName();
      if (name == null) {
        name = db.getClusterNameById(item.getCluster().getClusterNumber());
      }
      if (name != null) {
        result.add(name);
        return result;
      } else {
        return null;
      }
    } else if (item.getClusterList() != null) {
      for (OCluster cluster : item.getClusterList().toListOfClusters()) {
        String name = cluster.getClusterName();
        if (name == null) {
          name = db.getClusterNameById(cluster.getClusterNumber());
        }
        if (name != null) {
          result.add(name);
        }
      }
      return result;
    } else if (item.getIndex() != null) {
      String indexName = item.getIndex().getIndexName();
      OIndex<?> idx = db.getMetadata().getIndexManager().getIndex(indexName);
      result.addAll(idx.getClusters());
      if (result.isEmpty()) {
        return null;
      }
      return result;
    } else if (item.getInputParam() != null) {
      if (((ODatabaseInternal) ctx.getDatabase()).isSharded()) {
        throw new UnsupportedOperationException("Sharded query with input parameter as a target is not supported yet");
      }
      return null;
    } else if (item.getIdentifier() != null) {
      String className = item.getIdentifier().getStringValue();
      OClass clazz = db.getMetadata().getSchema().getClass(className);
      if (clazz == null) {
        return null;
      }
      int[] clusterIds = clazz.getPolymorphicClusterIds();
      for (int clusterId : clusterIds) {
        String clusterName = db.getClusterNameById(clusterId);
        if (clusterName != null) {
          result.add(clusterName);
        }
      }
      return result;
    }

    return null;
  }

  private OWhereClause translateLucene(OWhereClause whereClause) {
    if (whereClause == null) {
      return null;
    }

    if (whereClause.getBaseExpression() != null) {
      whereClause.getBaseExpression().translateLuceneOperator();
    }
    return whereClause;
  }

  /**
   * for backward compatibility, translate "distinct(foo)" to "DISTINCT foo".
   * This method modifies the projection itself.
   *
   * @param projection the projection
   */
  private OProjection translateDistinct(OProjection projection) {
    if (projection != null && projection.getItems().size() == 1) {
      if (isDistinct(projection.getItems().get(0))) {
        projection = projection.copy();
        OProjectionItem item = projection.getItems().get(0);
        OFunctionCall function = ((OBaseExpression) item.getExpression().getMathExpression()).getIdentifier().getLevelZero()
            .getFunctionCall();
        OExpression exp = function.getParams().get(0);
        OProjectionItem resultItem = new OProjectionItem(-1);
        resultItem.setAlias(item.getAlias());
        resultItem.setExpression(exp.copy());
        OProjection result = new OProjection(-1);
        result.setItems(new ArrayList<>());
        result.setDistinct(true);
        result.getItems().add(resultItem);
        return result;
      }
    }
    return projection;
  }

  /**
   * checks if a projection is a distinct(expr).
   * In new executor the distinct() function is not supported, so "distinct(expr)" is translated to "DISTINCT expr"
   *
   * @param item the projection
   *
   * @return
   */
  private boolean isDistinct(OProjectionItem item) {
    if (item.getExpression() == null) {
      return false;
    }
    if (item.getExpression().getMathExpression() == null) {
      return false;
    }
    if (!(item.getExpression().getMathExpression() instanceof OBaseExpression)) {
      return false;
    }
    OBaseExpression base = (OBaseExpression) item.getExpression().getMathExpression();
    if (base.getIdentifier() == null) {
      return false;
    }
    if (base.getModifier() != null) {
      return false;
    }
    if (base.getIdentifier().getLevelZero() == null) {
      return false;
    }
    OFunctionCall function = base.getIdentifier().getLevelZero().getFunctionCall();
    if (function == null) {
      return false;
    }
    if (function.getName().getStringValue().equalsIgnoreCase("distinct")) {
      return true;
    }
    return false;
  }

  private boolean handleHardwiredOptimizations(OSelectExecutionPlan result, OCommandContext ctx, boolean profilingEnabled) {
    return handleHardwiredCountOnIndex(result, info, ctx, profilingEnabled) || handleHardwiredCountOnClass(result, info, ctx,
        profilingEnabled);
  }

  private boolean handleHardwiredCountOnClass(OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx,
      boolean profilingEnabled) {
    OIdentifier targetClass = info.target == null ? null : info.target.getItem().getIdentifier();
    if (targetClass == null) {
      return false;
    }
    if (info.distinct || info.expand) {
      return false;
    }
    if (info.preAggregateProjection != null) {
      return false;
    }
    if (!isCountStar(info)) {
      return false;
    }
    if (!isMinimalQuery(info)) {
      return false;
    }
    result.chain(new CountFromClassStep(targetClass, info.projection.getAllAliases().iterator().next(), ctx, profilingEnabled));
    return true;
  }

  private boolean handleHardwiredCountOnIndex(OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx,
      boolean profilingEnabled) {
    OIndexIdentifier targetIndex = info.target == null ? null : info.target.getItem().getIndex();
    if (targetIndex == null) {
      return false;
    }
    if (info.distinct || info.expand) {
      return false;
    }
    if (info.preAggregateProjection != null) {
      return false;
    }
    if (!isCountStar(info)) {
      return false;
    }
    if (!isMinimalQuery(info)) {
      return false;
    }
    result.chain(new CountFromIndexStep(targetIndex, info.projection.getAllAliases().iterator().next(), ctx, profilingEnabled));
    return true;
  }

  /**
   * returns true if the query is minimal, ie. no WHERE condition, no SKIP/LIMIT, no UNWIND, no GROUP/ORDER BY, no LET
   *
   * @return
   */
  private boolean isMinimalQuery(QueryPlanningInfo info) {
    if (info.projectionAfterOrderBy != null || info.globalLetClause != null || info.perRecordLetClause != null
        || info.whereClause != null || info.flattenedWhereClause != null || info.groupBy != null || info.orderBy != null
        || info.unwind != null || info.skip != null || info.limit != null) {
      return false;
    }
    return true;
  }

  private boolean isCountStar(QueryPlanningInfo info) {
    if (info.aggregateProjection == null || info.projection == null || info.aggregateProjection.getItems().size() != 1
        || info.projection.getItems().size() != 1) {
      return false;
    }
    OProjectionItem item = info.aggregateProjection.getItems().get(0);
    if (!item.getExpression().toString().equalsIgnoreCase("count(*)")) {
      return false;
    }

    return true;
  }

  private boolean isCount(OProjection aggregateProjection, OProjection projection) {
    if (aggregateProjection == null || projection == null || aggregateProjection.getItems().size() != 1
        || projection.getItems().size() != 1) {
      return false;
    }
    OProjectionItem item = aggregateProjection.getItems().get(0);
    return item.getExpression().isCount();
  }

  private void handleUnwind(OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx, boolean profilingEnabled) {
    if (info.unwind != null) {
      result.chain(new UnwindStep(info.unwind, ctx, profilingEnabled));
    }
  }

  private void handleDistinct(OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx, boolean profilingEnabled) {
    result.chain(new DistinctExecutionStep(ctx, profilingEnabled));
  }

  private void handleProjectionsBeforeOrderBy(OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx,
      boolean profilingEnabled) {
    if (info.orderBy != null) {
      handleProjections(result, info, ctx, profilingEnabled);
    }
  }

  private void handleProjections(OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx,
      boolean profilingEnabled) {
    if (!info.projectionsCalculated && info.projection != null) {
      if (info.preAggregateProjection != null) {
        result.chain(new ProjectionCalculationStep(info.preAggregateProjection, ctx, profilingEnabled));
      }
      if (info.aggregateProjection != null) {
        result.chain(new AggregateProjectionCalculationStep(info.aggregateProjection, info.groupBy, ctx, profilingEnabled));
      }
      result.chain(new ProjectionCalculationStep(info.projection, ctx, profilingEnabled));

      info.projectionsCalculated = true;
    }
  }

  private void optimizeQuery(QueryPlanningInfo info) {
    splitLet(info);
    extractSubQueries(info);
    if (info.projection != null && info.projection.isExpand()) {
      info.expand = true;
      info.projection = info.projection.getExpandContent();
    }
    if (info.whereClause != null) {
      info.flattenedWhereClause = info.whereClause.flatten();
      //this helps index optimization
      info.flattenedWhereClause = moveFlattededEqualitiesLeft(info.flattenedWhereClause);
    }

    splitProjectionsForGroupBy(info);
    addOrderByProjections(info);
  }

  /**
   * splits LET clauses in global (executed once) and local (executed once per record)
   */
  private void splitLet(QueryPlanningInfo info) {
    if (info.perRecordLetClause != null && info.perRecordLetClause.getItems() != null) {
      Iterator<OLetItem> iterator = info.perRecordLetClause.getItems().iterator();
      while (iterator.hasNext()) {
        OLetItem item = iterator.next();
        if (item.getExpression() != null && item.getExpression().isEarlyCalculated()) {
          iterator.remove();
          addGlobalLet(info, item.getVarName(), item.getExpression());
        } else if (item.getQuery() != null && !item.getQuery().refersToParent()) {
          iterator.remove();
          addGlobalLet(info, item.getVarName(), item.getQuery());
        }
      }
    }
  }

  /**
   * re-writes a list of flat AND conditions, moving left all the equality operations
   *
   * @param flattenedWhereClause
   *
   * @return
   */
  private List<OAndBlock> moveFlattededEqualitiesLeft(List<OAndBlock> flattenedWhereClause) {
    if (flattenedWhereClause == null) {
      return null;
    }

    List<OAndBlock> result = new ArrayList<>();
    for (OAndBlock block : flattenedWhereClause) {
      List<OBooleanExpression> equalityExpressions = new ArrayList<>();
      List<OBooleanExpression> nonEqualityExpressions = new ArrayList<>();
      OAndBlock newBlock = block.copy();
      for (OBooleanExpression exp : newBlock.getSubBlocks()) {
        if (exp instanceof OBinaryCondition) {
          if (((OBinaryCondition) exp).getOperator() instanceof OEqualsCompareOperator) {
            equalityExpressions.add(exp);
          } else {
            nonEqualityExpressions.add(exp);
          }
        } else {
          nonEqualityExpressions.add(exp);
        }
      }
      OAndBlock newAnd = new OAndBlock(-1);
      newAnd.getSubBlocks().addAll(equalityExpressions);
      newAnd.getSubBlocks().addAll(nonEqualityExpressions);
      result.add(newAnd);
    }

    return result;
  }

  /**
   * creates additional projections for ORDER BY
   */
  private void addOrderByProjections(QueryPlanningInfo info) {
    if (info.orderApplied || info.expand || info.unwind != null || info.orderBy == null || info.orderBy.getItems().size() == 0
        || info.projection == null || info.projection.getItems() == null || (info.projection.getItems().size() == 1
        && info.projection.getItems().get(0).isAll())) {
      return;
    }

    OOrderBy newOrderBy = info.orderBy == null ? null : info.orderBy.copy();
    List<OProjectionItem> additionalOrderByProjections = calculateAdditionalOrderByProjections(info.projection.getAllAliases(),
        newOrderBy);
    if (additionalOrderByProjections.size() > 0) {
      info.orderBy = newOrderBy;//the ORDER BY has changed
    }
    if (additionalOrderByProjections.size() > 0) {
      info.projectionAfterOrderBy = new OProjection(-1);
      info.projectionAfterOrderBy.setItems(new ArrayList<>());
      for (String alias : info.projection.getAllAliases()) {
        info.projectionAfterOrderBy.getItems().add(projectionFromAlias(new OIdentifier(alias)));
      }

      for (OProjectionItem item : additionalOrderByProjections) {
        if (info.preAggregateProjection != null) {
          info.preAggregateProjection.getItems().add(item);
          info.aggregateProjection.getItems().add(projectionFromAlias(item.getAlias()));
          info.projection.getItems().add(projectionFromAlias(item.getAlias()));
        } else {
          info.projection.getItems().add(item);
        }
      }
    }
  }

  /**
   * given a list of aliases (present in the existing projections) calculates a list of additional projections to
   * add to the existing projections to allow ORDER BY calculation.
   * The sorting clause will be modified with new replaced aliases
   *
   * @param allAliases existing aliases in the projection
   * @param orderBy    sorting clause
   *
   * @return a list of additional projections to add to the existing projections to allow ORDER BY calculation (empty if nothing has
   * to be added).
   */
  private List<OProjectionItem> calculateAdditionalOrderByProjections(Set<String> allAliases, OOrderBy orderBy) {
    List<OProjectionItem> result = new ArrayList<>();
    int nextAliasCount = 0;
    if (orderBy != null && orderBy.getItems() != null || !orderBy.getItems().isEmpty()) {
      for (OOrderByItem item : orderBy.getItems()) {
        if (!allAliases.contains(item.getAlias())) {
          OProjectionItem newProj = new OProjectionItem(-1);
          if (item.getAlias() != null) {
            newProj.setExpression(new OExpression(new OIdentifier(item.getAlias()), item.getModifier()));
          } else if (item.getRecordAttr() != null) {
            ORecordAttribute attr = new ORecordAttribute(-1);
            attr.setName(item.getRecordAttr());
            newProj.setExpression(new OExpression(attr, item.getModifier()));
          } else if (item.getRid() != null) {
            OExpression exp = new OExpression(-1);
            exp.setRid(item.getRid().copy());
            newProj.setExpression(exp);
          }
          OIdentifier newAlias = new OIdentifier("_$$$ORDER_BY_ALIAS$$$_" + (nextAliasCount++));
          newProj.setAlias(newAlias);
          item.setAlias(newAlias.getStringValue());
          result.add(newProj);
        }
      }
    }
    return result;
  }

  /**
   * splits projections in three parts (pre-aggregate, aggregate and final) to efficiently manage aggregations
   */
  private void splitProjectionsForGroupBy(QueryPlanningInfo info) {
    if (info.projection == null) {
      return;
    }

    OProjection preAggregate = new OProjection(-1);
    preAggregate.setItems(new ArrayList<>());
    OProjection aggregate = new OProjection(-1);
    aggregate.setItems(new ArrayList<>());
    OProjection postAggregate = new OProjection(-1);
    postAggregate.setItems(new ArrayList<>());

    boolean isSplitted = false;

    //split for aggregate projections
    AggregateProjectionSplit result = new AggregateProjectionSplit();
    for (OProjectionItem item : info.projection.getItems()) {
      result.reset();
      if (isAggregate(item)) {
        isSplitted = true;
        OProjectionItem post = item.splitForAggregation(result);
        OIdentifier postAlias = item.getProjectionAlias();
        postAlias.setQuoted(true);
        post.setAlias(postAlias);
        postAggregate.getItems().add(post);
        aggregate.getItems().addAll(result.getAggregate());
        preAggregate.getItems().addAll(result.getPreAggregate());
      } else {
        preAggregate.getItems().add(item);
        //also push the alias forward in the chain
        OProjectionItem aggItem = new OProjectionItem(-1);
        aggItem.setExpression(new OExpression(item.getProjectionAlias()));
        aggregate.getItems().add(aggItem);
        postAggregate.getItems().add(aggItem);
      }
    }

    //bind split projections to the execution planner
    if (isSplitted) {
      info.preAggregateProjection = preAggregate;
      if (info.preAggregateProjection.getItems() == null || info.preAggregateProjection.getItems().size() == 0) {
        info.preAggregateProjection = null;
      }
      info.aggregateProjection = aggregate;
      if (info.aggregateProjection.getItems() == null || info.aggregateProjection.getItems().size() == 0) {
        info.aggregateProjection = null;
      }
      info.projection = postAggregate;

      addGroupByExpressionsToProjections(info);
    }
  }

  private boolean isAggregate(OProjectionItem item) {
    if (item.isAggregate()) {
      return true;
    }
    return false;
  }

  private OProjectionItem projectionFromAlias(OIdentifier oIdentifier) {
    OProjectionItem result = new OProjectionItem(-1);
    result.setExpression(new OExpression(oIdentifier));
    return result;
  }

  /**
   * if GROUP BY is performed on an expression that is not explicitly in the pre-aggregate projections, then
   * that expression has to be put in the pre-aggregate (only here, in subsequent steps it's removed)
   */
  private void addGroupByExpressionsToProjections(QueryPlanningInfo info) {
    if (info.groupBy == null || info.groupBy.getItems() == null || info.groupBy.getItems().size() == 0) {
      return;
    }
    OGroupBy newGroupBy = new OGroupBy(-1);
    int i = 0;
    for (OExpression exp : info.groupBy.getItems()) {
      if (exp.isAggregate()) {
        throw new OCommandExecutionException("Cannot group by an aggregate function");
      }
      boolean found = false;
      for (String alias : info.preAggregateProjection.getAllAliases()) {
        if (alias.equals(exp.getDefaultAlias().getStringValue())) {
          found = true;
          newGroupBy.getItems().add(exp);
          break;
        }
      }
      if (!found) {
        OProjectionItem newItem = new OProjectionItem(-1);
        newItem.setExpression(exp);
        OIdentifier groupByAlias = new OIdentifier(-1);
        groupByAlias.setStringValue("_$$$GROUP_BY_ALIAS$$$_" + i);
        newItem.setAlias(groupByAlias);
        info.preAggregateProjection.getItems().add(newItem);
        newGroupBy.getItems().add(new OExpression(groupByAlias));
      }

      info.groupBy = newGroupBy;
    }

  }

  /**
   * translates subqueries to LET statements
   */
  private void extractSubQueries(QueryPlanningInfo info) {
    SubQueryCollector collector = new SubQueryCollector();
    if (info.perRecordLetClause != null) {
      info.perRecordLetClause.extractSubQueries(collector);
    }
    int i = 0;
    int j = 0;
    for (Map.Entry<OIdentifier, OStatement> entry : collector.getSubQueries().entrySet()) {
      OIdentifier alias = entry.getKey();
      OStatement query = entry.getValue();
      if (query.refersToParent()) {
        addRecordLevelLet(info, alias, query, j++);
      } else {
        addGlobalLet(info, alias, query, i++);
      }
    }
    collector.reset();

    if (info.whereClause != null) {
      info.whereClause.extractSubQueries(collector);
    }
    if (info.projection != null) {
      info.projection.extractSubQueries(collector);
    }
    if (info.orderBy != null) {
      info.orderBy.extractSubQueries(collector);
    }
    if (info.groupBy != null) {
      info.groupBy.extractSubQueries(collector);
    }

    for (Map.Entry<OIdentifier, OStatement> entry : collector.getSubQueries().entrySet()) {
      OIdentifier alias = entry.getKey();
      OStatement query = entry.getValue();
      if (query.refersToParent()) {
        addRecordLevelLet(info, alias, query);
      } else {
        addGlobalLet(info, alias, query);
      }
    }
  }

  private void addGlobalLet(QueryPlanningInfo info, OIdentifier alias, OExpression exp) {
    if (info.globalLetClause == null) {
      info.globalLetClause = new OLetClause(-1);
    }
    OLetItem item = new OLetItem(-1);
    item.setVarName(alias);
    item.setExpression(exp);
    info.globalLetClause.addItem(item);
  }

  private void addGlobalLet(QueryPlanningInfo info, OIdentifier alias, OStatement stm) {
    if (info.globalLetClause == null) {
      info.globalLetClause = new OLetClause(-1);
    }
    OLetItem item = new OLetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    info.globalLetClause.addItem(item);
  }

  private void addGlobalLet(QueryPlanningInfo info, OIdentifier alias, OStatement stm, int pos) {
    if (info.globalLetClause == null) {
      info.globalLetClause = new OLetClause(-1);
    }
    OLetItem item = new OLetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    info.globalLetClause.getItems().add(pos, item);
  }

  private void addRecordLevelLet(QueryPlanningInfo info, OIdentifier alias, OStatement stm) {
    if (info.perRecordLetClause == null) {
      info.perRecordLetClause = new OLetClause(-1);
    }
    OLetItem item = new OLetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    info.perRecordLetClause.addItem(item);
  }

  private void addRecordLevelLet(QueryPlanningInfo info, OIdentifier alias, OStatement stm, int pos) {
    if (info.perRecordLetClause == null) {
      info.perRecordLetClause = new OLetClause(-1);
    }
    OLetItem item = new OLetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    info.perRecordLetClause.getItems().add(pos, item);
  }

  private void handleFetchFromTarger(OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx,
      boolean profilingEnabled) {

    OFromItem target = info.target == null ? null : info.target.getItem();
    if (target == null) {
      handleNoTarget(result, ctx, profilingEnabled);
    } else if (target.getIdentifier() != null) {
      handleClassAsTarget(result, info, ctx, profilingEnabled);
    } else if (target.getCluster() != null) {
      handleClustersAsTarget(result, info, Collections.singletonList(target.getCluster()), ctx, profilingEnabled);
    } else if (target.getClusterList() != null) {
      handleClustersAsTarget(result, info, target.getClusterList().toListOfClusters(), ctx, profilingEnabled);
    } else if (target.getStatement() != null) {
      handleSubqueryAsTarget(result, target.getStatement(), ctx, profilingEnabled);
    } else if (target.getFunctionCall() != null) {
      //        handleFunctionCallAsTarget(result, target.getFunctionCall(), ctx);//TODO
      throw new OCommandExecutionException("function call as target is not supported yet");
    } else if (target.getInputParam() != null) {
      handleInputParamAsTarget(result, info, target.getInputParam(), ctx, profilingEnabled);
    } else if (target.getInputParams() != null && target.getInputParams().size() > 0) {
      List<OInternalExecutionPlan> plans = new ArrayList<>();
      for (OInputParameter param : target.getInputParams()) {
        OSelectExecutionPlan subPlan = new OSelectExecutionPlan(ctx);
        handleInputParamAsTarget(subPlan, info, param, ctx, profilingEnabled);
        plans.add(subPlan);
      }
      result.chain(new ParallelExecStep(plans, ctx, profilingEnabled));
    } else if (target.getIndex() != null) {
      handleIndexAsTarget(result, info, target.getIndex(), ctx, profilingEnabled);
    } else if (target.getMetadata() != null) {
      handleMetadataAsTarget(result, target.getMetadata(), ctx, profilingEnabled);
    } else if (target.getRids() != null && target.getRids().size() > 0) {
      handleRidsAsTarget(result, target.getRids(), ctx, profilingEnabled);
    } else {
      throw new UnsupportedOperationException();
    }

  }

  private void handleInputParamAsTarget(OSelectExecutionPlan result, QueryPlanningInfo info, OInputParameter inputParam,
      OCommandContext ctx, boolean profilingEnabled) {
    Object paramValue = inputParam.getValue(ctx.getInputParameters());
    if (paramValue == null) {
      result.chain(new EmptyStep(ctx, profilingEnabled));//nothing to return
    } else if (paramValue instanceof OClass) {
      OFromClause from = new OFromClause(-1);
      OFromItem item = new OFromItem(-1);
      from.setItem(item);
      item.setIdentifier(new OIdentifier(((OClass) paramValue).getName()));
      handleClassAsTarget(result, from, info, ctx, profilingEnabled);
    } else if (paramValue instanceof String) {
      //strings are treated as classes
      OFromClause from = new OFromClause(-1);
      OFromItem item = new OFromItem(-1);
      from.setItem(item);
      item.setIdentifier(new OIdentifier((String) paramValue));
      handleClassAsTarget(result, from, info, ctx, profilingEnabled);
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

      handleRidsAsTarget(result, Collections.singletonList(rid), ctx, profilingEnabled);
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
      handleRidsAsTarget(result, rids, ctx, profilingEnabled);
    } else {
      throw new OCommandExecutionException("Invalid target: " + paramValue);
    }
  }

  private void handleNoTarget(OSelectExecutionPlan result, OCommandContext ctx, boolean profilingEnabled) {
    result.chain(new EmptyDataGeneratorStep(1, ctx, profilingEnabled));
  }

  private void handleIndexAsTarget(OSelectExecutionPlan result, QueryPlanningInfo info, OIndexIdentifier indexIdentifier,
      OCommandContext ctx, boolean profilingEnabled) {
    String indexName = indexIdentifier.getIndexName();
    OIndex<?> index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(indexName);
    if (index == null) {
      throw new OCommandExecutionException("Index not found: " + indexName);
    }

    switch (indexIdentifier.getType()) {
    case INDEX:
      OBooleanExpression keyCondition = null;
      OBooleanExpression ridCondition = null;
      if (info.flattenedWhereClause == null || info.flattenedWhereClause.size() == 0) {
        if (!index.supportsOrderedIterations()) {
          throw new OCommandExecutionException("Index " + indexName + " does not allow iteration without a condition");
        }
      } else if (info.flattenedWhereClause.size() > 1) {
        throw new OCommandExecutionException(
            "Index queries with this kind of condition are not supported yet: " + info.whereClause);
      } else {
        OAndBlock andBlock = info.flattenedWhereClause.get(0);
        if (andBlock.getSubBlocks().size() == 1) {

          info.whereClause = null;//The WHERE clause won't be used anymore, the index does all the filtering
          info.flattenedWhereClause = null;
          keyCondition = getKeyCondition(andBlock);
          if (keyCondition == null) {
            throw new OCommandExecutionException(
                "Index queries with this kind of condition are not supported yet: " + info.whereClause);
          }
        } else if (andBlock.getSubBlocks().size() == 2) {
          info.whereClause = null;//The WHERE clause won't be used anymore, the index does all the filtering
          info.flattenedWhereClause = null;
          keyCondition = getKeyCondition(andBlock);
          ridCondition = getRidCondition(andBlock);
          if (keyCondition == null || ridCondition == null) {
            throw new OCommandExecutionException(
                "Index queries with this kind of condition are not supported yet: " + info.whereClause);
          }
        } else {
          throw new OCommandExecutionException(
              "Index queries with this kind of condition are not supported yet: " + info.whereClause);
        }
      }
      result.chain(new FetchFromIndexStep(index, keyCondition, null, ctx, profilingEnabled));
      if (ridCondition != null) {
        OWhereClause where = new OWhereClause(-1);
        where.setBaseExpression(ridCondition);
        result.chain(new FilterStep(where, ctx, profilingEnabled));
      }
      break;
    case VALUES:
    case VALUESASC:
      if (!index.supportsOrderedIterations()) {
        throw new OCommandExecutionException("Index " + indexName + " does not allow iteration on values");
      }
      result.chain(new FetchFromIndexValuesStep(index, true, ctx, profilingEnabled));
      result.chain(new GetValueFromIndexEntryStep(ctx, profilingEnabled));
      break;
    case VALUESDESC:
      if (!index.supportsOrderedIterations()) {
        throw new OCommandExecutionException("Index " + indexName + " does not allow iteration on values");
      }
      result.chain(new FetchFromIndexValuesStep(index, false, ctx, profilingEnabled));
      result.chain(new GetValueFromIndexEntryStep(ctx, profilingEnabled));
      break;
    }
  }

  private OBooleanExpression getKeyCondition(OAndBlock andBlock) {
    for (OBooleanExpression exp : andBlock.getSubBlocks()) {
      String str = exp.toString();
      if (str.length() < 5) {
        continue;
      }
      if (str.substring(0, 4).equalsIgnoreCase("key ")) {
        return exp;
      }
    }
    return null;
  }

  private OBooleanExpression getRidCondition(OAndBlock andBlock) {
    for (OBooleanExpression exp : andBlock.getSubBlocks()) {
      String str = exp.toString();
      if (str.length() < 5) {
        continue;
      }
      if (str.substring(0, 4).equalsIgnoreCase("rid ")) {
        return exp;
      }
    }
    return null;
  }

  private void handleMetadataAsTarget(OSelectExecutionPlan plan, OMetadataIdentifier metadata, OCommandContext ctx,
      boolean profilingEnabled) {
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
    plan.chain(new FetchFromRidsStep(Collections.singleton(schemaRid), ctx, profilingEnabled));

  }

  private void handleRidsAsTarget(OSelectExecutionPlan plan, List<ORid> rids, OCommandContext ctx, boolean profilingEnabled) {
    List<ORecordId> actualRids = new ArrayList<>();
    for (ORid rid : rids) {
      actualRids.add(rid.toRecordId((OResult) null, ctx));
    }
    plan.chain(new FetchFromRidsStep(actualRids, ctx, profilingEnabled));
  }

  private void handleExpand(OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx, boolean profilingEnabled) {
    if (info.expand) {
      result.chain(new ExpandStep(ctx, profilingEnabled));
    }
  }

  private void handleGlobalLet(OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx, boolean profilingEnabled) {
    if (info.globalLetClause != null) {
      List<OLetItem> items = info.globalLetClause.getItems();
      for (OLetItem item : items) {
        if (item.getExpression() != null) {
          result.chain(new GlobalLetExpressionStep(item.getVarName(), item.getExpression(), ctx, profilingEnabled));
        } else {
          result.chain(new GlobalLetQueryStep(item.getVarName(), item.getQuery(), ctx, profilingEnabled));
        }
        info.globalLetPresent = true;
      }
    }
  }

  private void handleLet(OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx, boolean profilingEnabled) {
    if (info.perRecordLetClause != null) {
      List<OLetItem> items = info.perRecordLetClause.getItems();
      for (OLetItem item : items) {
        if (item.getExpression() != null) {
          result.chain(new LetExpressionStep(item.getVarName(), item.getExpression(), ctx, profilingEnabled));
        } else {
          result.chain(new LetQueryStep(item.getVarName(), item.getQuery(), ctx, profilingEnabled));
        }
      }
    }
  }

  private void handleWhere(OSelectExecutionPlan plan, QueryPlanningInfo info, OCommandContext ctx, boolean profilingEnabled) {
    if (info.whereClause != null) {
      plan.chain(new FilterStep(info.whereClause, ctx, profilingEnabled));
    }
  }

  private void handleOrderBy(OSelectExecutionPlan plan, QueryPlanningInfo info, OCommandContext ctx, boolean profilingEnabled) {
    int skipSize = info.skip == null ? 0 : info.skip.getValue(ctx);
    if (skipSize < 0) {
      throw new OCommandExecutionException("Cannot execute a query with a negative SKIP");
    }
    int limitSize = info.limit == null ? -1 : info.limit.getValue(ctx);
    Integer maxResults = null;
    if (limitSize >= 0) {
      maxResults = skipSize + limitSize;
    }
    if (info.expand || info.unwind != null) {
      maxResults = null;
    }
    if (!info.orderApplied && info.orderBy != null && info.orderBy.getItems() != null && info.orderBy.getItems().size() > 0) {
      plan.chain(new OrderByStep(info.orderBy, maxResults, ctx, profilingEnabled));
      if (info.projectionAfterOrderBy != null) {
        plan.chain(new ProjectionCalculationStep(info.projectionAfterOrderBy, ctx, profilingEnabled));
      }
    }
  }

  private void handleClassAsTarget(OSelectExecutionPlan plan, QueryPlanningInfo info, OCommandContext ctx,
      boolean profilingEnabled) {
    handleClassAsTarget(plan, info.target, info, ctx, profilingEnabled);
  }

  private void handleClassAsTarget(OSelectExecutionPlan plan, OFromClause from, QueryPlanningInfo info, OCommandContext ctx,
      boolean profilingEnabled) {
    OIdentifier identifier = from.getItem().getIdentifier();
    if (handleClassAsTargetWithIndexedFunction(plan, identifier, info, ctx, profilingEnabled)) {
      plan.chain(new FilterByClassStep(identifier, ctx, profilingEnabled));
      return;
    }

    if (handleClassAsTargetWithIndex(plan, identifier, info, ctx, profilingEnabled)) {
      plan.chain(new FilterByClassStep(identifier, ctx, profilingEnabled));
      return;
    }

    if (info.orderBy != null && handleClassWithIndexForSortOnly(plan, identifier, info, ctx, profilingEnabled)) {
      plan.chain(new FilterByClassStep(identifier, ctx, profilingEnabled));
      return;
    }

    Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    if (isOrderByRidAsc(info)) {
      orderByRidAsc = true;
    } else if (isOrderByRidDesc(info)) {
      orderByRidAsc = false;
    }
    FetchFromClassExecutionStep fetcher = new FetchFromClassExecutionStep(identifier.getStringValue(), ctx, orderByRidAsc,
        profilingEnabled);
    if (orderByRidAsc != null) {
      info.orderApplied = true;
    }
    plan.chain(fetcher);
  }

  private boolean handleClassAsTargetWithIndexedFunction(OSelectExecutionPlan plan, OIdentifier queryTarget, QueryPlanningInfo info,
      OCommandContext ctx, boolean profilingEnabled) {
    if (queryTarget == null) {
      return false;
    }
    OClass clazz = ctx.getDatabase().getMetadata().getSchema().getClass(queryTarget.getStringValue());
    if (clazz == null) {
      throw new OCommandExecutionException("Class not found: " + queryTarget);
    }
    if (info.flattenedWhereClause == null || info.flattenedWhereClause.size() == 0) {
      return false;
    }

    List<OInternalExecutionPlan> resultSubPlans = new ArrayList<>();

    boolean indexedFunctionsFound = false;

    for (OAndBlock block : info.flattenedWhereClause) {
      List<OBinaryCondition> indexedFunctionConditions = block
          .getIndexedFunctionConditions(clazz, (ODatabaseDocumentInternal) ctx.getDatabase());

      indexedFunctionConditions = filterIndexedFunctionsWithoutIndex(indexedFunctionConditions, info.target, ctx);

      if (indexedFunctionConditions == null || indexedFunctionConditions.size() == 0) {
        IndexSearchDescriptor bestIndex = findBestIndexFor(ctx, clazz.getIndexes(), block, clazz);
        if (bestIndex != null) {

          FetchFromIndexStep step = new FetchFromIndexStep(bestIndex.idx, bestIndex.keyCondition,
              bestIndex.additionalRangeCondition, true, ctx, profilingEnabled);

          OSelectExecutionPlan subPlan = new OSelectExecutionPlan(ctx);
          subPlan.chain(step);
          subPlan.chain(new GetValueFromIndexEntryStep(ctx, profilingEnabled));
          if (!block.getSubBlocks().isEmpty()) {
            subPlan.chain(new FilterStep(createWhereFrom(block), ctx, profilingEnabled));
          }
          resultSubPlans.add(subPlan);
        } else {
          FetchFromClassExecutionStep step = new FetchFromClassExecutionStep(clazz.getName(), ctx, true, profilingEnabled);
          OSelectExecutionPlan subPlan = new OSelectExecutionPlan(ctx);
          subPlan.chain(step);
          if (!block.getSubBlocks().isEmpty()) {
            subPlan.chain(new FilterStep(createWhereFrom(block), ctx, profilingEnabled));
          }
          resultSubPlans.add(subPlan);
        }
      } else {
        OBinaryCondition blockCandidateFunction = null;
        for (OBinaryCondition cond : indexedFunctionConditions) {
          if (!cond.allowsIndexedFunctionExecutionOnTarget(info.target, ctx)) {
            if (!cond.canExecuteIndexedFunctionWithoutIndex(info.target, ctx)) {
              throw new OCommandExecutionException("Cannot execute " + block + " on " + queryTarget);
            }
          }
          if (blockCandidateFunction == null) {
            blockCandidateFunction = cond;
          } else {
            boolean thisAllowsNoIndex = cond.canExecuteIndexedFunctionWithoutIndex(info.target, ctx);
            boolean prevAllowsNoIndex = blockCandidateFunction.canExecuteIndexedFunctionWithoutIndex(info.target, ctx);
            if (!thisAllowsNoIndex && !prevAllowsNoIndex) {
              //none of the functions allow execution without index, so cannot choose one
              throw new OCommandExecutionException(
                  "Cannot choose indexed function between " + cond + " and " + blockCandidateFunction
                      + ". Both require indexed execution");
            } else if (thisAllowsNoIndex && prevAllowsNoIndex) {
              //both can be calculated without index, choose the best one for index execution
              long thisEstimate = cond.estimateIndexed(info.target, ctx);
              long lastEstimate = blockCandidateFunction.estimateIndexed(info.target, ctx);
              if (thisEstimate > -1 && thisEstimate < lastEstimate) {
                blockCandidateFunction = cond;
              }
            } else if (prevAllowsNoIndex) {
              //choose current condition, because the other one can be calculated without index
              blockCandidateFunction = cond;
            }
          }
        }

        FetchFromIndexedFunctionStep step = new FetchFromIndexedFunctionStep(blockCandidateFunction, info.target, ctx,
            profilingEnabled);
        if (!blockCandidateFunction.executeIndexedFunctionAfterIndexSearch(info.target, ctx)) {
          block = block.copy();
          block.getSubBlocks().remove(blockCandidateFunction);
        }
        if (info.flattenedWhereClause.size() == 1) {
          plan.chain(step);
          if (!block.getSubBlocks().isEmpty()) {
            plan.chain(new FilterStep(createWhereFrom(block), ctx, profilingEnabled));
          }
        } else {
          OSelectExecutionPlan subPlan = new OSelectExecutionPlan(ctx);
          subPlan.chain(step);
          if (!block.getSubBlocks().isEmpty()) {
            subPlan.chain(new FilterStep(createWhereFrom(block), ctx, profilingEnabled));
          }
          resultSubPlans.add(subPlan);
        }
        indexedFunctionsFound = true;
      }
    }

    if (indexedFunctionsFound) {
      if (resultSubPlans.size() > 1) { //if resultSubPlans.size() == 1 the step was already chained (see above)
        plan.chain(new ParallelExecStep(resultSubPlans, ctx, profilingEnabled));
        plan.chain(new DistinctExecutionStep(ctx, profilingEnabled));
      }
      //WHERE condition already applied
      info.whereClause = null;
      info.flattenedWhereClause = null;
      return true;
    } else {
      return false;
    }
  }

  private List<OBinaryCondition> filterIndexedFunctionsWithoutIndex(List<OBinaryCondition> indexedFunctionConditions,
      OFromClause fromClause, OCommandContext ctx) {
    if (indexedFunctionConditions == null) {
      return null;
    }
    List<OBinaryCondition> result = new ArrayList<>();
    for (OBinaryCondition cond : indexedFunctionConditions) {
      if (cond.allowsIndexedFunctionExecutionOnTarget(fromClause, ctx)) {
        result.add(cond);
      } else if (!cond.canExecuteIndexedFunctionWithoutIndex(fromClause, ctx)) {
        throw new OCommandExecutionException("Cannot evaluate " + cond + ": no index defined");
      }
    }
    return result;
  }

  /**
   * tries to use an index for sorting only. Also adds the fetch step to the execution plan
   *
   * @param plan current execution plan
   * @param info the query planning information
   * @param ctx  the current context
   *
   * @return true if it succeeded to use an index to sort, false otherwise.
   */

  private boolean handleClassWithIndexForSortOnly(OSelectExecutionPlan plan, OIdentifier queryTarget, QueryPlanningInfo info,
      OCommandContext ctx, boolean profilingEnabled) {

    OClass clazz = ctx.getDatabase().getMetadata().getSchema().getClass(queryTarget.getStringValue());
    if (clazz == null) {
      throw new OCommandExecutionException("Class not found: " + queryTarget.getStringValue());
    }

    for (OIndex idx : clazz.getIndexes().stream().filter(i -> i.supportsOrderedIterations()).filter(i -> i.getDefinition() != null)
        .collect(Collectors.toList())) {
      List<String> indexFields = idx.getDefinition().getFields();
      if (indexFields.size() < info.orderBy.getItems().size()) {
        continue;
      }
      boolean indexFound = true;
      String orderType = null;
      for (int i = 0; i < info.orderBy.getItems().size(); i++) {
        OOrderByItem orderItem = info.orderBy.getItems().get(i);
        String indexField = indexFields.get(i);
        if (i == 0) {
          orderType = orderItem.getType();
        } else {
          if (orderType == null || !orderType.equals(orderItem.getType())) {
            indexFound = false;
            break;//ASC/DESC interleaved, cannot be used with index.
          }
        }
        if (!indexField.equals(orderItem.getAlias())) {
          indexFound = false;
          break;
        }
      }
      if (indexFound && orderType != null) {
        plan.chain(new FetchFromIndexValuesStep(idx, orderType.equals(OOrderByItem.ASC), ctx, profilingEnabled));
        plan.chain(new GetValueFromIndexEntryStep(ctx, profilingEnabled));
        info.orderApplied = true;
        return true;
      }
    }
    return false;
  }

  private boolean handleClassAsTargetWithIndex(OSelectExecutionPlan plan, OIdentifier targetClass, QueryPlanningInfo info,
      OCommandContext ctx, boolean profilingEnabled) {

    List<OExecutionStepInternal> result = handleClassAsTargetWithIndex(targetClass.getStringValue(), info, ctx, profilingEnabled);
    if (result != null) {
      result.stream().forEach(x -> plan.chain(x));
      info.whereClause = null;
      info.flattenedWhereClause = null;
      return true;
    }
    OClass clazz = ctx.getDatabase().getMetadata().getSchema().getClass(targetClass.getStringValue());
    if (clazz == null) {
      throw new OCommandExecutionException("Cannot find class " + targetClass);
    }
    if (clazz.count(false) != 0 || clazz.getSubclasses().size() == 0 || isDiamondHierarchy(clazz)) {
      return false;
    }
    //try subclasses

    Collection<OClass> subclasses = clazz.getSubclasses();

    List<OInternalExecutionPlan> subclassPlans = new ArrayList<>();
    for (OClass subClass : subclasses) {
      List<OExecutionStepInternal> subSteps = handleClassAsTargetWithIndexRecursive(subClass.getName(), info, ctx,
          profilingEnabled);
      if (subSteps == null || subSteps.size() == 0) {
        return false;
      }
      OSelectExecutionPlan subPlan = new OSelectExecutionPlan(ctx);
      subSteps.stream().forEach(x -> subPlan.chain(x));
      subclassPlans.add(subPlan);
    }
    if (subclassPlans.size() > 0) {
      plan.chain(new ParallelExecStep(subclassPlans, ctx, profilingEnabled));
      return true;
    }
    return false;
  }

  /**
   * checks if a class is the top of a diamond hierarchy
   *
   * @param clazz
   *
   * @return
   */
  private boolean isDiamondHierarchy(OClass clazz) {
    Set<OClass> traversed = new HashSet<>();
    List<OClass> stack = new ArrayList<>();
    stack.add(clazz);
    while (!stack.isEmpty()) {
      OClass current = stack.remove(0);
      traversed.add(current);
      for (OClass sub : current.getSubclasses()) {
        if (traversed.contains(sub)) {
          return true;
        }
        stack.add(sub);
        traversed.add(sub);
      }
    }
    return false;
  }

  private List<OExecutionStepInternal> handleClassAsTargetWithIndexRecursive(String targetClass, QueryPlanningInfo info,
      OCommandContext ctx, boolean profilingEnabled) {
    List<OExecutionStepInternal> result = handleClassAsTargetWithIndex(targetClass, info, ctx, profilingEnabled);
    if (result == null) {
      result = new ArrayList<>();
      OClass clazz = ctx.getDatabase().getMetadata().getSchema().getClass(targetClass);
      if (clazz == null) {
        throw new OCommandExecutionException("Cannot find class " + targetClass);
      }
      if (clazz.count(false) != 0 || clazz.getSubclasses().size() == 0 || isDiamondHierarchy(clazz)) {
        return null;
      }

      Collection<OClass> subclasses = clazz.getSubclasses();

      List<OInternalExecutionPlan> subclassPlans = new ArrayList<>();
      for (OClass subClass : subclasses) {
        List<OExecutionStepInternal> subSteps = handleClassAsTargetWithIndexRecursive(subClass.getName(), info, ctx,
            profilingEnabled);
        if (subSteps == null || subSteps.size() == 0) {
          return null;
        }
        OSelectExecutionPlan subPlan = new OSelectExecutionPlan(ctx);
        subSteps.stream().forEach(x -> subPlan.chain(x));
        subclassPlans.add(subPlan);
      }
      if (subclassPlans.size() > 0) {
        result.add(new ParallelExecStep(subclassPlans, ctx, profilingEnabled));
      }
    }
    return result.size() == 0 ? null : result;
  }

  private List<OExecutionStepInternal> handleClassAsTargetWithIndex(String targetClass, QueryPlanningInfo info, OCommandContext ctx,
      boolean profilingEnabled) {
    if (info.flattenedWhereClause == null || info.flattenedWhereClause.size() == 0) {
      return null;
    }

    OClass clazz = ctx.getDatabase().getMetadata().getSchema().getClass(targetClass);
    if (clazz == null) {
      throw new OCommandExecutionException("Cannot find class " + targetClass);
    }

    Set<OIndex<?>> indexes = clazz.getIndexes();

    List<IndexSearchDescriptor> indexSearchDescriptors = info.flattenedWhereClause.stream()
        .map(x -> findBestIndexFor(ctx, indexes, x, clazz)).filter(Objects::nonNull).collect(Collectors.toList());
    if (indexSearchDescriptors.size() != info.flattenedWhereClause.size()) {
      return null; //some blocks could not be managed with an index
    }

    List<OExecutionStepInternal> result = null;
    List<IndexSearchDescriptor> optimumIndexSearchDescriptors = commonFactor(indexSearchDescriptors);

    if (indexSearchDescriptors.size() == 1) {
      IndexSearchDescriptor desc = indexSearchDescriptors.get(0);
      result = new ArrayList<>();
      Boolean orderAsc = getOrderDirection(info);
      result.add(
          new FetchFromIndexStep(desc.idx, desc.keyCondition, desc.additionalRangeCondition, !Boolean.FALSE.equals(orderAsc), ctx,
              profilingEnabled));
      result.add(new GetValueFromIndexEntryStep(ctx, profilingEnabled));
      if (orderAsc != null && info.orderBy != null && fullySorted(info.orderBy, desc.keyCondition, desc.idx)) {
        info.orderApplied = true;
      }
      if (desc.remainingCondition != null && !desc.remainingCondition.isEmpty()) {
        result.add(new FilterStep(createWhereFrom(desc.remainingCondition), ctx, profilingEnabled));
      }
    } else {
      result = new ArrayList<>();
      result.add(createParallelIndexFetch(optimumIndexSearchDescriptors, ctx, profilingEnabled));
    }
    return result;
  }

  private boolean fullySorted(OOrderBy orderBy, OAndBlock conditions, OIndex idx) {
    if (!idx.supportsOrderedIterations())
      return false;

    List<String> orderItems = new ArrayList<>();
    String order = null;

    for (OOrderByItem item : orderBy.getItems()) {
      if (order == null) {
        order = item.getType();
      } else if (!order.equals(item.getType())) {
        return false;
      }
      orderItems.add(item.getAlias());
    }

    List<String> conditionItems = new ArrayList<>();

    for (int i = 0; i < conditions.getSubBlocks().size(); i++) {
      OBooleanExpression item = conditions.getSubBlocks().get(i);
      if (item instanceof OBinaryCondition) {
        if (((OBinaryCondition) item).getOperator() instanceof OEqualsCompareOperator) {
          conditionItems.add(((OBinaryCondition) item).getLeft().toString());
        } else if (i != conditions.getSubBlocks().size() - 1) {
          return false;
        }

      } else if (i != conditions.getSubBlocks().size() - 1) {
        return false;
      }
    }

    List<String> orderedFields = new ArrayList<>();
    boolean overlapping = false;
    for (String s : conditionItems) {
      if (orderItems.isEmpty()) {
        return true;//nothing to sort, the conditions completely overlap the ORDER BY
      }
      if (s.equals(orderItems.get(0))) {
        orderItems.remove(0);
        overlapping = true; //start overlapping
      } else if (overlapping) {
        return false; //overlapping, but next order item does not match...
      }
      orderedFields.add(s);
    }
    orderedFields.addAll(orderItems);

    final OIndexDefinition definition = idx.getDefinition();
    final List<String> fields = definition.getFields();
    if (fields.size() < orderedFields.size()) {
      return false;
    }

    for (int i = 0; i < orderedFields.size(); i++) {
      final String orderFieldName = orderedFields.get(i);
      final String indexFieldName = fields.get(i);
      if (!orderFieldName.equals(indexFieldName)) {
        return false;
      }
    }

    return true;
  }

  /**
   * returns TRUE if all the order clauses are ASC, FALSE if all are DESC, null otherwise
   *
   * @return TRUE if all the order clauses are ASC, FALSE if all are DESC, null otherwise
   */
  private Boolean getOrderDirection(QueryPlanningInfo info) {
    if (info.orderBy == null) {
      return null;
    }
    String result = null;
    for (OOrderByItem item : info.orderBy.getItems()) {
      if (result == null) {
        result = item.getType() == null ? OOrderByItem.ASC : item.getType();
      } else {
        String newType = item.getType() == null ? OOrderByItem.ASC : item.getType();
        if (!newType.equals(result)) {
          return null;
        }
      }
    }
    return result == null || result.equals(OOrderByItem.ASC) ? true : false;
  }

  private OExecutionStepInternal createParallelIndexFetch(List<IndexSearchDescriptor> indexSearchDescriptors, OCommandContext ctx,
      boolean profilingEnabled) {
    List<OInternalExecutionPlan> subPlans = new ArrayList<>();
    for (IndexSearchDescriptor desc : indexSearchDescriptors) {
      OSelectExecutionPlan subPlan = new OSelectExecutionPlan(ctx);
      subPlan.chain(new FetchFromIndexStep(desc.idx, desc.keyCondition, desc.additionalRangeCondition, ctx, profilingEnabled));
      subPlan.chain(new GetValueFromIndexEntryStep(ctx, profilingEnabled));
      if (desc.remainingCondition != null && !desc.remainingCondition.isEmpty()) {
        subPlan.chain(new FilterStep(createWhereFrom(desc.remainingCondition), ctx, profilingEnabled));
      }
      subPlans.add(subPlan);
    }
    return new ParallelExecStep(subPlans, ctx, profilingEnabled);
  }

  private OWhereClause createWhereFrom(OBooleanExpression remainingCondition) {
    OWhereClause result = new OWhereClause(-1);
    result.setBaseExpression(remainingCondition);
    return result;
  }

  /**
   * given a flat AND block and a set of indexes, returns the best index to be used to process it, with the complete description on
   * how to use it
   *
   * @param ctx
   * @param indexes
   * @param block
   *
   * @return
   */
  private IndexSearchDescriptor findBestIndexFor(OCommandContext ctx, Set<OIndex<?>> indexes, OAndBlock block, OClass clazz) {
    return indexes.stream().filter(x -> x.getInternal().canBeUsedInEqualityOperators())
        .map(index -> buildIndexSearchDescriptor(ctx, index, block, clazz)).filter(Objects::nonNull)
        .filter(x -> x.keyCondition != null).filter(x -> x.keyCondition.getSubBlocks().size() > 0)
        .min(Comparator.comparing(x -> x.cost(ctx))).orElse(null);
  }

  /**
   * given an index and a flat AND block, returns a descriptor on how to process it with an index (index, index key and additional
   * filters to apply after index fetch
   *
   * @param ctx
   * @param index
   * @param block
   * @param clazz
   *
   * @return
   */
  private IndexSearchDescriptor buildIndexSearchDescriptor(OCommandContext ctx, OIndex<?> index, OAndBlock block, OClass clazz) {
    List<String> indexFields = index.getDefinition().getFields();
    OBinaryCondition keyCondition = new OBinaryCondition(-1);
    OIdentifier key = new OIdentifier(-1);
    key.setStringValue("key");
    keyCondition.setLeft(new OExpression(key));
    boolean allowsRange = allowsRangeQueries(index);
    boolean found = false;

    OAndBlock blockCopy = block.copy();
    Iterator<OBooleanExpression> blockIterator;

    OAndBlock indexKeyValue = new OAndBlock(-1);
    IndexSearchDescriptor result = new IndexSearchDescriptor();
    result.idx = index;
    result.keyCondition = indexKeyValue;
    for (String indexField : indexFields) {
      blockIterator = blockCopy.getSubBlocks().iterator();
      boolean breakHere = false;
      while (blockIterator.hasNext()) {
        OBooleanExpression singleExp = blockIterator.next();
        if (singleExp instanceof OBinaryCondition) {
          OExpression left = ((OBinaryCondition) singleExp).getLeft();
          if (left.isBaseIdentifier()) {
            String fieldName = left.getDefaultAlias().getStringValue();
            if (indexField.equals(fieldName)) {
              OBinaryCompareOperator operator = ((OBinaryCondition) singleExp).getOperator();
              if (!((OBinaryCondition) singleExp).getRight().isEarlyCalculated()) {
                continue; //this cannot be used because the value depends on single record
              }
              if (operator instanceof OEqualsCompareOperator) {
                found = true;
                OBinaryCondition condition = new OBinaryCondition(-1);
                condition.setLeft(left);
                condition.setOperator(operator);
                condition.setRight(((OBinaryCondition) singleExp).getRight().copy());
                indexKeyValue.getSubBlocks().add(condition);
                blockIterator.remove();
                break;
              } else if (allowsRange && operator.isRangeOperator()) {
                found = true;
                breakHere = true;//this is last element, no other fields can be added to the key because this is a range condition
                OBinaryCondition condition = new OBinaryCondition(-1);
                condition.setLeft(left);
                condition.setOperator(operator);
                condition.setRight(((OBinaryCondition) singleExp).getRight().copy());
                indexKeyValue.getSubBlocks().add(condition);
                blockIterator.remove();
                //look for the opposite condition, on the same field, for range queries (the other side of the range)
                while (blockIterator.hasNext()) {
                  OBooleanExpression next = blockIterator.next();
                  if (createsRangeWith((OBinaryCondition) singleExp, next)) {
                    result.additionalRangeCondition = (OBinaryCondition) next;
                    blockIterator.remove();
                    break;
                  }
                }
                break;
              }
            }
          }
        }
      }
      if (breakHere) {
        break;
      }
    }

    if (result.keyCondition.getSubBlocks().size() < index.getDefinition().getFields().size() && !index
        .supportsOrderedIterations()) {
      //hash indexes do not support partial key match
      return null;
    }

    if (found) {
      result.remainingCondition = blockCopy;
      return result;
    }
    return null;
  }

  private boolean createsRangeWith(OBinaryCondition left, OBooleanExpression next) {
    if (!(next instanceof OBinaryCondition)) {
      return false;
    }
    OBinaryCondition right = (OBinaryCondition) next;
    if (!left.getLeft().equals(right.getLeft())) {
      return false;
    }
    OBinaryCompareOperator leftOperator = left.getOperator();
    OBinaryCompareOperator rightOperator = right.getOperator();
    if (leftOperator instanceof OGeOperator || leftOperator instanceof OGtOperator) {
      return rightOperator instanceof OLeOperator || rightOperator instanceof OLtOperator;
    }
    if (leftOperator instanceof OLeOperator || leftOperator instanceof OLtOperator) {
      return rightOperator instanceof OGeOperator || rightOperator instanceof OGtOperator;
    }
    return false;
  }

  private boolean allowsRangeQueries(OIndex<?> index) {
    return index.supportsOrderedIterations();
  }

  /**
   * aggregates multiple index conditions that refer to the same key search
   *
   * @param indexSearchDescriptors
   *
   * @return
   */
  private List<IndexSearchDescriptor> commonFactor(List<IndexSearchDescriptor> indexSearchDescriptors) {
    //index, key condition, additional filter (to aggregate in OR)
    Map<OIndex, Map<IndexCondPair, OOrBlock>> aggregation = new HashMap<>();
    for (IndexSearchDescriptor item : indexSearchDescriptors) {
      Map<IndexCondPair, OOrBlock> filtersForIndex = aggregation.get(item.idx);
      if (filtersForIndex == null) {
        filtersForIndex = new HashMap<>();
        aggregation.put(item.idx, filtersForIndex);
      }
      IndexCondPair extendedCond = new IndexCondPair(item.keyCondition, item.additionalRangeCondition);

      OOrBlock existingAdditionalConditions = filtersForIndex.get(extendedCond);
      if (existingAdditionalConditions == null) {
        existingAdditionalConditions = new OOrBlock(-1);
        filtersForIndex.put(extendedCond, existingAdditionalConditions);
      }
      existingAdditionalConditions.getSubBlocks().add(item.remainingCondition);
    }
    List<IndexSearchDescriptor> result = new ArrayList<>();
    for (Map.Entry<OIndex, Map<IndexCondPair, OOrBlock>> item : aggregation.entrySet()) {
      for (Map.Entry<IndexCondPair, OOrBlock> filters : item.getValue().entrySet()) {
        result.add(new IndexSearchDescriptor(item.getKey(), filters.getKey().mainCondition, filters.getKey().additionalRange,
            filters.getValue()));
      }
    }
    return result;
  }

  private void handleClustersAsTarget(OSelectExecutionPlan plan, QueryPlanningInfo info, List<OCluster> clusters,
      OCommandContext ctx, boolean profilingEnabled) {
    ODatabase db = ctx.getDatabase();
    Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    if (isOrderByRidAsc(info)) {
      orderByRidAsc = true;
    } else if (isOrderByRidDesc(info)) {
      orderByRidAsc = false;
    }
    if (orderByRidAsc != null) {
      info.orderApplied = true;
    }
    if (clusters.size() == 1) {
      OCluster cluster = clusters.get(0);
      Integer clusterId = cluster.getClusterNumber();
      if (clusterId == null) {
        clusterId = db.getClusterIdByName(cluster.getClusterName());
      }
      if (clusterId == null) {
        throw new OCommandExecutionException("Cluster " + cluster + " does not exist");
      }
      FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(clusterId, ctx, profilingEnabled);
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
      FetchFromClustersExecutionStep step = new FetchFromClustersExecutionStep(clusterIds, ctx, orderByRidAsc, profilingEnabled);
      plan.chain(step);
    }
  }

  private void handleSubqueryAsTarget(OSelectExecutionPlan plan, OStatement subQuery, OCommandContext ctx,
      boolean profilingEnabled) {
    OBasicCommandContext subCtx = new OBasicCommandContext();
    subCtx.setDatabase(ctx.getDatabase());
    subCtx.setParent(ctx);
    OInternalExecutionPlan subExecutionPlan = subQuery.createExecutionPlan(subCtx, profilingEnabled);
    plan.chain(new SubQueryStep(subExecutionPlan, ctx, subCtx, profilingEnabled));
  }

  private boolean isOrderByRidDesc(QueryPlanningInfo info) {
    if (!hasTargetWithSortedRids(info)) {
      return false;
    }

    if (info.orderBy == null) {
      return false;
    }
    if (info.orderBy.getItems().size() == 1) {
      OOrderByItem item = info.orderBy.getItems().get(0);
      String recordAttr = item.getRecordAttr();
      if (recordAttr != null && recordAttr.equalsIgnoreCase("@rid") && OOrderByItem.DESC.equals(item.getType())) {
        return true;
      }
    }
    return false;
  }

  private boolean isOrderByRidAsc(QueryPlanningInfo info) {
    if (!hasTargetWithSortedRids(info)) {
      return false;
    }

    if (info.orderBy == null) {
      return false;
    }
    if (info.orderBy.getItems().size() == 1) {
      OOrderByItem item = info.orderBy.getItems().get(0);
      String recordAttr = item.getRecordAttr();
      if (recordAttr != null && recordAttr.equalsIgnoreCase("@rid") && (item.getType() == null || OOrderByItem.ASC
          .equals(item.getType()))) {
        return true;
      }
    }
    return false;
  }

  private boolean hasTargetWithSortedRids(QueryPlanningInfo info) {
    if (info.target == null) {
      return false;
    }
    if (info.target.getItem() == null) {
      return false;
    }
    if (info.target.getItem().getIdentifier() != null) {
      return true;
    } else if (info.target.getItem().getCluster() != null) {
      return true;
    } else if (info.target.getItem().getClusterList() != null) {
      return true;
    }
    return false;
  }

}
