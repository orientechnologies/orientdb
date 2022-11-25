package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexAbstract;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.parser.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OSelectExecutionPlanner {

  private QueryPlanningInfo info;
  private OSelectStatement statement;

  public OSelectExecutionPlanner(OSelectStatement oSelectStatement) {
    this.statement = oSelectStatement;
  }

  private void init(OCommandContext ctx) {
    // copying the content, so that it can be manipulated and optimized
    info = new QueryPlanningInfo();
    info.projection =
        this.statement.getProjection() == null ? null : this.statement.getProjection().copy();
    info.projection = translateDistinct(info.projection);
    info.distinct = info.projection != null && info.projection.isDistinct();
    if (info.projection != null) {
      info.projection.setDistinct(false);
    }

    info.target = this.statement.getTarget();
    info.whereClause =
        this.statement.getWhereClause() == null ? null : this.statement.getWhereClause().copy();
    info.whereClause = translateLucene(info.whereClause);
    info.perRecordLetClause =
        this.statement.getLetClause() == null ? null : this.statement.getLetClause().copy();
    info.groupBy = this.statement.getGroupBy() == null ? null : this.statement.getGroupBy().copy();
    info.orderBy = this.statement.getOrderBy() == null ? null : this.statement.getOrderBy().copy();
    info.unwind = this.statement.getUnwind() == null ? null : this.statement.getUnwind().copy();
    info.skip = this.statement.getSkip();
    info.limit = this.statement.getLimit();
    info.lockRecord = this.statement.getLockRecord();
    info.timeout = this.statement.getTimeout() == null ? null : this.statement.getTimeout().copy();
    if (info.timeout == null
        && ctx.getDatabase().getConfiguration().getValueAsLong(OGlobalConfiguration.COMMAND_TIMEOUT)
            > 0) {
      info.timeout = new OTimeout(-1);
      info.timeout.setVal(
          ctx.getDatabase()
              .getConfiguration()
              .getValueAsLong(OGlobalConfiguration.COMMAND_TIMEOUT));
    }
  }

  public OInternalExecutionPlan createExecutionPlan(
      OCommandContext ctx, boolean enableProfiling, boolean useCache) {
    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) ctx.getDatabase();
    if (useCache && !enableProfiling && statement.executinPlanCanBeCached()) {
      OExecutionPlan plan = OExecutionPlanCache.get(statement.getOriginalStatement(), ctx, db);
      if (plan != null) {
        return (OInternalExecutionPlan) plan;
      }
    }

    long planningStart = System.currentTimeMillis();

    init(ctx);
    OSelectExecutionPlan result = new OSelectExecutionPlan(ctx);

    if (info.expand && info.distinct) {
      throw new OCommandExecutionException(
          "Cannot execute a statement with DISTINCT expand(), please use a subquery");
    }

    optimizeQuery(info, ctx);

    if (handleHardwiredOptimizations(result, ctx, enableProfiling)) {
      return result;
    }

    handleGlobalLet(result, info, ctx, enableProfiling);

    calculateShardingStrategy(info, ctx);

    handleFetchFromTarger(result, info, ctx, enableProfiling);

    if (info.globalLetPresent) {
      // do the raw fetch remotely, then do the rest on the coordinator
      buildDistributedExecutionPlan(result, info, ctx, enableProfiling);
    }

    handleLet(result, info, ctx, enableProfiling);

    handleWhere(result, info, ctx, enableProfiling);

    // TODO optimization: in most cases the projections can be calculated on remote nodes
    buildDistributedExecutionPlan(result, info, ctx, enableProfiling);

    handleLockRecord(result, info, ctx, enableProfiling);

    handleProjectionsBlock(result, info, ctx, enableProfiling);

    if (info.timeout != null) {
      result.chain(new AccumulatingTimeoutStep(info.timeout, ctx, enableProfiling));
    }

    if (useCache
        && !enableProfiling
        && statement.executinPlanCanBeCached()
        && result.canBeCached()
        && OExecutionPlanCache.getLastInvalidation(db) < planningStart) {
      OExecutionPlanCache.put(
          statement.getOriginalStatement(), result, (ODatabaseDocumentInternal) ctx.getDatabase());
    }
    return result;
  }

  private void handleLockRecord(
      OSelectExecutionPlan result,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean enableProfiling) {
    if (info.lockRecord != null) {
      result.chain(new LockRecordStep(info.lockRecord, ctx, enableProfiling));
    }
  }

  public static void handleProjectionsBlock(
      OSelectExecutionPlan result,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean enableProfiling) {
    handleProjectionsBeforeOrderBy(result, info, ctx, enableProfiling);

    if (info.expand || info.unwind != null || info.groupBy != null) {

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
      if (info.distinct || info.groupBy != null || info.aggregateProjection != null) {
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
  }

  private void buildDistributedExecutionPlan(
      OSelectExecutionPlan result,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean enableProfiling) {
    if (info.distributedFetchExecutionPlans == null) {
      return;
    }
    String currentNode = ((ODatabaseDocumentInternal) ctx.getDatabase()).getLocalNodeName();
    if (info.distributedFetchExecutionPlans.size() == 1) {
      if (info.distributedFetchExecutionPlans.get(currentNode) != null) {
        // everything is executed on local server
        OSelectExecutionPlan localSteps = info.distributedFetchExecutionPlans.get(currentNode);
        for (OExecutionStep step : localSteps.getSteps()) {
          result.chain((OExecutionStepInternal) step);
        }
      } else {
        // everything is executed on a single remote node
        String node = info.distributedFetchExecutionPlans.keySet().iterator().next();
        OSelectExecutionPlan subPlan = info.distributedFetchExecutionPlans.get(node);
        DistributedExecutionStep step =
            new DistributedExecutionStep(subPlan, node, ctx, enableProfiling);
        result.chain(step);
      }
      info.distributedFetchExecutionPlans = null;
    } else {
      // sharded fetching
      List<OExecutionPlan> subPlans = new ArrayList<>();
      for (Map.Entry<String, OSelectExecutionPlan> entry :
          info.distributedFetchExecutionPlans.entrySet()) {
        if (entry.getKey().equals(currentNode)) {
          subPlans.add(entry.getValue());
        } else {
          DistributedExecutionStep step =
              new DistributedExecutionStep(entry.getValue(), entry.getKey(), ctx, enableProfiling);
          OSelectExecutionPlan subPlan = new OSelectExecutionPlan(ctx);
          subPlan.chain(step);
          subPlans.add(subPlan);
        }
      }
      result.chain(new ParallelExecStep((List) subPlans, ctx, enableProfiling));
    }
    info.distributedPlanCreated = true;
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
    info.distributedFetchExecutionPlans = new LinkedHashMap<>();
    String localNode = db.getLocalNodeName();
    Collection<String> readClusterNames = db.getClusterNames();
    Set<String> clusterNames;
    if (readClusterNames instanceof Set) {
      clusterNames = (Set<String>) readClusterNames;
    } else {
      clusterNames = new HashSet<>(readClusterNames);
    }
    if (!db.isSharded()) {
      info.serverToClusters = new LinkedHashMap<>();
      info.serverToClusters.put(localNode, clusterNames);
      info.distributedFetchExecutionPlans.put(localNode, new OSelectExecutionPlan(ctx));
      return;
    }

    //    Map<String, Set<String>> clusterMap = db.getActiveClusterMap();
    Map<String, Set<String>> clusterMap = new HashMap<>();
    clusterMap.put(localNode, new HashSet<>(clusterNames));

    Set<String> queryClusters = calculateTargetClusters(info, ctx);
    if (queryClusters == null || queryClusters.size() == 0) { // no target

      info.serverToClusters = new LinkedHashMap<>();
      info.serverToClusters.put(localNode, clusterMap.get(localNode));
      info.distributedFetchExecutionPlans.put(localNode, new OSelectExecutionPlan(ctx));
      return;
    }

    //    Set<String> serversWithAllTheClusers = getServersThatHasAllClusters(clusterMap,
    // queryClusters);
    //    if (serversWithAllTheClusers.isEmpty()) {
    // sharded query
    Map<String, Set<String>> minimalSetOfNodes =
        getMinimalSetOfNodesForShardedQuery(db.getLocalNodeName(), clusterMap, queryClusters);
    if (minimalSetOfNodes == null) {
      throw new OCommandExecutionException("Cannot execute sharded query");
    }
    info.serverToClusters = minimalSetOfNodes;
    for (String node : info.serverToClusters.keySet()) {
      info.distributedFetchExecutionPlans.put(node, new OSelectExecutionPlan(ctx));
    }
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
   * given a cluster map and a set of clusters involved in a query, tries to calculate the minimum
   * number of nodes that will have to be involved in the query execution, with clusters involved
   * for each node.
   *
   * @param clusterMap
   * @param queryClusters
   * @return a map that has node names as a key and clusters (data files) for each node as a value
   */
  private Map<String, Set<String>> getMinimalSetOfNodesForShardedQuery(
      String localNode, Map<String, Set<String>> clusterMap, Set<String> queryClusters) {
    // approximate algorithm, the problem is NP-complete
    Map<String, Set<String>> result = new LinkedHashMap<>();
    Set<String> uncovered = new HashSet<>();
    uncovered.addAll(queryClusters);
    uncovered =
        uncovered.stream()
            .filter(x -> x != null)
            .map(x -> x.toLowerCase(Locale.ENGLISH))
            .collect(Collectors.toSet());

    // try local node first
    Set<String> nextNodeClusters = new HashSet<>();
    Set<String> clustersForNode = clusterMap.get(localNode);
    if (clustersForNode != null) {
      nextNodeClusters.addAll(clustersForNode);
    }
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
      if (nextNodeClusters.size() == 0) {
        throw new OCommandExecutionException(
            "Cannot execute a sharded query: clusters ["
                + uncovered.stream().collect(Collectors.joining(", "))
                + "] are not present on any node"
                + "\n ["
                + clusterMap.entrySet().stream()
                    .map(
                        x ->
                            ""
                                + x.getKey()
                                + ":("
                                + x.getValue().stream().collect(Collectors.joining(","))
                                + ")")
                    .collect(Collectors.joining(", "))
                + "]");
      }
      result.put(nextNode, nextNodeClusters);
      uncovered.removeAll(nextNodeClusters);
    }
    return result;
  }

  private String findItemThatCoversMore(
      Set<String> uncovered, Map<String, Set<String>> clusterMap) {
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
   * @param clusterMap the cluster map for current sharding configuration
   * @param queryClusters the clusters that are target of the query
   * @return
   */
  private Set<String> getServersThatHasAllClusters(
      Map<String, Set<String>> clusterMap, Set<String> queryClusters) {
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
   * @return a set of cluster names this query will fetch from
   */
  private Set<String> calculateTargetClusters(QueryPlanningInfo info, OCommandContext ctx) {
    if (info.target == null) {
      return Collections.EMPTY_SET;
    }

    Set<String> result = new HashSet<>();
    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) ctx.getDatabase();
    OFromItem item = info.target.getItem();
    if (item.getRids() != null && item.getRids().size() > 0) {
      if (item.getRids().size() == 1) {
        OInteger cluster = item.getRids().get(0).getCluster();
        if (cluster.getValue().longValue() > ORID.CLUSTER_MAX) {
          throw new OCommandExecutionException(
              "Invalid cluster Id:" + cluster + ". Max allowed value = " + ORID.CLUSTER_MAX);
        }
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
        throw new UnsupportedOperationException(
            "Sharded query with input parameter as a target is not supported yet");
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
      OIndex idx = db.getMetadata().getIndexManagerInternal().getIndex(db, indexName);
      if (idx == null) {
        throw new OCommandExecutionException("Index " + indexName + " does not exist");
      }
      result.addAll(idx.getClusters());
      if (result.isEmpty()) {
        return null;
      }
      return result;
    } else if (item.getInputParam() != null) {
      if (((ODatabaseInternal) ctx.getDatabase()).isSharded()) {
        throw new UnsupportedOperationException(
            "Sharded query with input parameter as a target is not supported yet");
      }
      return null;
    } else if (item.getIdentifier() != null) {
      String className = item.getIdentifier().getStringValue();
      OClass clazz = getSchemaFromContext(ctx).getClass(className);
      if (clazz == null) {
        clazz = getSchemaFromContext(ctx).getView(className);
      }
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
   * for backward compatibility, translate "distinct(foo)" to "DISTINCT foo". This method modifies
   * the projection itself.
   *
   * @param projection the projection
   */
  protected static OProjection translateDistinct(OProjection projection) {
    if (projection != null && projection.getItems().size() == 1) {
      if (isDistinct(projection.getItems().get(0))) {
        projection = projection.copy();
        OProjectionItem item = projection.getItems().get(0);
        OFunctionCall function =
            ((OBaseExpression) item.getExpression().getMathExpression())
                .getIdentifier()
                .getLevelZero()
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
   * checks if a projection is a distinct(expr). In new executor the distinct() function is not
   * supported, so "distinct(expr)" is translated to "DISTINCT expr"
   *
   * @param item the projection
   * @return
   */
  private static boolean isDistinct(OProjectionItem item) {
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
    return function.getName().getStringValue().equalsIgnoreCase("distinct");
  }

  private boolean handleHardwiredOptimizations(
      OSelectExecutionPlan result, OCommandContext ctx, boolean profilingEnabled) {
    if (handleHardwiredCountOnIndex(result, info, ctx, profilingEnabled)) {
      return true;
    }
    if (handleHardwiredCountOnClass(result, info, ctx, profilingEnabled)) {
      return true;
    }
    return handleHardwiredCountOnClassUsingIndex(result, info, ctx, profilingEnabled);
  }

  private boolean handleHardwiredCountOnClass(
      OSelectExecutionPlan result,
      QueryPlanningInfo info,
      OCommandContext ctx,
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
    if (securityPoliciesExistForClass(targetClass, ctx)) {
      return false;
    }
    result.chain(
        new CountFromClassStep(
            targetClass, info.projection.getAllAliases().iterator().next(), ctx, profilingEnabled));
    return true;
  }

  private boolean securityPoliciesExistForClass(OIdentifier targetClass, OCommandContext ctx) {
    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) ctx.getDatabase();
    OSecurityInternal security = db.getSharedContext().getSecurity();
    OClass clazz =
        db.getMetadata()
            .getImmutableSchemaSnapshot()
            .getClass(targetClass.getStringValue()); // normalize class name case
    if (clazz == null) {
      return false;
    }
    return security.isReadRestrictedBySecurityPolicy(
        (ODatabaseSession) db, "database.class." + clazz.getName());
  }

  private boolean handleHardwiredCountOnClassUsingIndex(
      OSelectExecutionPlan result,
      QueryPlanningInfo info,
      OCommandContext ctx,
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
    if (info.projectionAfterOrderBy != null
        || info.globalLetClause != null
        || info.perRecordLetClause != null
        || info.groupBy != null
        || info.orderBy != null
        || info.unwind != null
        || info.skip != null) {
      return false;
    }
    OClass clazz =
        ((ODatabaseDocumentInternal) ctx.getDatabase())
            .getMetadata()
            .getImmutableSchemaSnapshot()
            .getClass(targetClass.getStringValue());
    if (clazz == null) {
      return false;
    }
    if (info.flattenedWhereClause == null
        || info.flattenedWhereClause.size() > 1
        || info.flattenedWhereClause.get(0).getSubBlocks().size() > 1) {
      // for now it only handles a single equality condition, it can be extended
      return false;
    }
    OBooleanExpression condition = info.flattenedWhereClause.get(0).getSubBlocks().get(0);
    if (!(condition instanceof OBinaryCondition)) {
      return false;
    }
    OBinaryCondition binaryCondition = (OBinaryCondition) condition;
    if (!binaryCondition.getLeft().isBaseIdentifier()) {
      return false;
    }
    if (!(binaryCondition.getOperator() instanceof OEqualsCompareOperator)) {
      // this can be extended to use range operators too
      return false;
    }
    if (securityPoliciesExistForClass(targetClass, ctx)) {
      return false;
    }

    for (OIndex classIndex : clazz.getClassIndexes()) {
      List<String> fields = classIndex.getDefinition().getFields();
      if (fields.size() == 1
          && fields.get(0).equals(binaryCondition.getLeft().getDefaultAlias().getStringValue())) {
        OExpression expr = ((OBinaryCondition) condition).getRight();
        result.chain(
            new CountFromIndexWithKeyStep(
                new OIndexIdentifier(classIndex.getName(), OIndexIdentifier.Type.INDEX),
                expr,
                info.projection.getAllAliases().iterator().next(),
                ctx,
                profilingEnabled));
        return true;
      }
    }

    return false;
  }

  private boolean handleHardwiredCountOnIndex(
      OSelectExecutionPlan result,
      QueryPlanningInfo info,
      OCommandContext ctx,
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
    result.chain(
        new CountFromIndexStep(
            targetIndex, info.projection.getAllAliases().iterator().next(), ctx, profilingEnabled));
    return true;
  }

  /**
   * returns true if the query is minimal, ie. no WHERE condition, no SKIP/LIMIT, no UNWIND, no
   * GROUP/ORDER BY, no LET
   *
   * @return
   */
  private boolean isMinimalQuery(QueryPlanningInfo info) {
    return info.projectionAfterOrderBy == null
        && info.globalLetClause == null
        && info.perRecordLetClause == null
        && info.whereClause == null
        && info.flattenedWhereClause == null
        && info.groupBy == null
        && info.orderBy == null
        && info.unwind == null
        && info.skip == null;
  }

  private static boolean isCountStar(QueryPlanningInfo info) {
    if (info.aggregateProjection == null
        || info.projection == null
        || info.aggregateProjection.getItems().size() != 1
        || info.projection.getItems().size() != 1) {
      return false;
    }
    OProjectionItem item = info.aggregateProjection.getItems().get(0);
    return item.getExpression().toString().equalsIgnoreCase("count(*)");
  }

  private static boolean isCountOnly(QueryPlanningInfo info) {
    if (info.aggregateProjection == null
        || info.projection == null
        || info.aggregateProjection.getItems().size() != 1
        || info.projection.getItems().stream()
                .filter(x -> !x.getProjectionAliasAsString().startsWith("_$$$ORDER_BY_ALIAS$$$_"))
                .count()
            != 1) {
      return false;
    }
    OProjectionItem item = info.aggregateProjection.getItems().get(0);
    OExpression exp = item.getExpression();
    if (exp.getMathExpression() != null && exp.getMathExpression() instanceof OBaseExpression) {
      OBaseExpression base = (OBaseExpression) exp.getMathExpression();
      return base.isCount() && base.getModifier() == null;
    }
    return false;
  }

  public static void handleUnwind(
      OSelectExecutionPlan result,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {
    if (info.unwind != null) {
      result.chain(new UnwindStep(info.unwind, ctx, profilingEnabled));
    }
  }

  private static void handleDistinct(
      OSelectExecutionPlan result,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {
    if (info.distinct) {
      result.chain(new DistinctExecutionStep(ctx, profilingEnabled));
    }
  }

  private static void handleProjectionsBeforeOrderBy(
      OSelectExecutionPlan result,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {
    if (info.orderBy != null) {
      handleProjections(result, info, ctx, profilingEnabled);
    }
  }

  private static void handleProjections(
      OSelectExecutionPlan result,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {
    if (!info.projectionsCalculated && info.projection != null) {
      if (info.preAggregateProjection != null) {
        result.chain(
            new ProjectionCalculationStep(info.preAggregateProjection, ctx, profilingEnabled));
      }
      if (info.aggregateProjection != null) {
        long aggregationLimit = -1;
        if (info.orderBy == null && info.limit != null) {
          aggregationLimit = info.limit.getValue(ctx);
          if (info.skip != null && info.skip.getValue(ctx) > 0) {
            aggregationLimit += info.skip.getValue(ctx);
          }
        }
        result.chain(
            new AggregateProjectionCalculationStep(
                info.aggregateProjection,
                info.groupBy,
                aggregationLimit,
                ctx,
                info.timeout != null ? info.timeout.getVal().longValue() : -1,
                profilingEnabled));
        if (isCountOnly(info) && info.groupBy == null) {
          result.chain(
              new GuaranteeEmptyCountStep(
                  info.aggregateProjection.getItems().get(0), ctx, profilingEnabled));
        }
      }
      result.chain(new ProjectionCalculationStep(info.projection, ctx, profilingEnabled));

      info.projectionsCalculated = true;
    }
  }

  protected static void optimizeQuery(QueryPlanningInfo info, OCommandContext ctx) {
    splitLet(info, ctx);
    rewriteIndexChainsAsSubqueries(info, ctx);
    extractSubQueries(info);
    if (info.projection != null && info.projection.isExpand()) {
      info.expand = true;
      info.projection = info.projection.getExpandContent();
    }
    if (info.whereClause != null) {
      info.flattenedWhereClause = info.whereClause.flatten();
      // this helps index optimization
      info.flattenedWhereClause = moveFlattededEqualitiesLeft(info.flattenedWhereClause);
    }

    splitProjectionsForGroupBy(info, ctx);
    addOrderByProjections(info);
  }

  private static void rewriteIndexChainsAsSubqueries(QueryPlanningInfo info, OCommandContext ctx) {
    if (ctx == null || ctx.getDatabase() == null) {
      return;
    }
    if (info.whereClause != null
        && info.target != null
        && info.target.getItem().getIdentifier() != null) {
      String className = info.target.getItem().getIdentifier().getStringValue();
      OSchema schema = getSchemaFromContext(ctx);
      OClass clazz = schema.getClass(className);
      if (clazz == null) {
        clazz = schema.getView(className);
      }
      if (clazz != null) {
        info.whereClause.getBaseExpression().rewriteIndexChainsAsSubqueries(ctx, clazz);
      }
    }
  }

  /** splits LET clauses in global (executed once) and local (executed once per record) */
  private static void splitLet(QueryPlanningInfo info, OCommandContext ctx) {
    if (info.perRecordLetClause != null && info.perRecordLetClause.getItems() != null) {
      Iterator<OLetItem> iterator = info.perRecordLetClause.getItems().iterator();
      while (iterator.hasNext()) {
        OLetItem item = iterator.next();
        if (item.getExpression() != null
            && (item.getExpression().isEarlyCalculated(ctx)
                || isUnionAllOfQueries(info, item.getVarName(), item.getExpression()))) {
          iterator.remove();
          addGlobalLet(info, item.getVarName(), item.getExpression());
        } else if (item.getQuery() != null && !item.getQuery().refersToParent()) {
          iterator.remove();
          addGlobalLet(info, item.getVarName(), item.getQuery());
        }
      }
    }
  }

  private static boolean isUnionAllOfQueries(
      QueryPlanningInfo info, OIdentifier varName, OExpression expression) {
    if (expression.getMathExpression() instanceof OBaseExpression) {
      OBaseExpression exp = (OBaseExpression) expression.getMathExpression();
      if (exp.getIdentifier() != null
          && exp.getModifier() == null
          && exp.getIdentifier().getLevelZero() != null
          && exp.getIdentifier().getLevelZero().getFunctionCall() != null) {
        OFunctionCall fc = exp.getIdentifier().getLevelZero().getFunctionCall();
        if (fc.getName().getStringValue().equalsIgnoreCase("unionall")) {
          for (OExpression param : fc.getParams()) {
            if (param.toString().startsWith("$")) {
              return true;
            }
          }
          return true;
        }
      }
    }
    return false;
  }

  /**
   * re-writes a list of flat AND conditions, moving left all the equality operations
   *
   * @param flattenedWhereClause
   * @return
   */
  private static List<OAndBlock> moveFlattededEqualitiesLeft(List<OAndBlock> flattenedWhereClause) {
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

  /** creates additional projections for ORDER BY */
  private static void addOrderByProjections(QueryPlanningInfo info) {
    if (info.orderApplied
        || info.expand
        || info.unwind != null
        || info.orderBy == null
        || info.orderBy.getItems().size() == 0
        || info.projection == null
        || info.projection.getItems() == null
        || (info.projection.getItems().size() == 1 && info.projection.getItems().get(0).isAll())) {
      return;
    }

    OOrderBy newOrderBy = info.orderBy == null ? null : info.orderBy.copy();
    List<OProjectionItem> additionalOrderByProjections =
        calculateAdditionalOrderByProjections(info.projection.getAllAliases(), newOrderBy);
    if (additionalOrderByProjections.size() > 0) {
      info.orderBy = newOrderBy; // the ORDER BY has changed
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
   * given a list of aliases (present in the existing projections) calculates a list of additional
   * projections to add to the existing projections to allow ORDER BY calculation. The sorting
   * clause will be modified with new replaced aliases
   *
   * @param allAliases existing aliases in the projection
   * @param orderBy sorting clause
   * @return a list of additional projections to add to the existing projections to allow ORDER BY
   *     calculation (empty if nothing has to be added).
   */
  private static List<OProjectionItem> calculateAdditionalOrderByProjections(
      Set<String> allAliases, OOrderBy orderBy) {
    List<OProjectionItem> result = new ArrayList<>();
    int nextAliasCount = 0;
    if (orderBy != null && orderBy.getItems() != null || !orderBy.getItems().isEmpty()) {
      for (OOrderByItem item : orderBy.getItems()) {
        if (!allAliases.contains(item.getAlias())) {
          OProjectionItem newProj = new OProjectionItem(-1);
          if (item.getAlias() != null) {
            newProj.setExpression(
                new OExpression(new OIdentifier(item.getAlias()), item.getModifier()));
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
          item.setModifier(null);
          result.add(newProj);
        }
      }
    }
    return result;
  }

  /**
   * splits projections in three parts (pre-aggregate, aggregate and final) to efficiently manage
   * aggregations
   */
  private static void splitProjectionsForGroupBy(QueryPlanningInfo info, OCommandContext ctx) {
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

    // split for aggregate projections
    AggregateProjectionSplit result = new AggregateProjectionSplit();
    for (OProjectionItem item : info.projection.getItems()) {
      result.reset();
      if (isAggregate(item)) {
        isSplitted = true;
        OProjectionItem post = item.splitForAggregation(result, ctx);
        OIdentifier postAlias = item.getProjectionAlias();
        postAlias = new OIdentifier(postAlias, true);
        post.setAlias(postAlias);
        postAggregate.getItems().add(post);
        aggregate.getItems().addAll(result.getAggregate());
        preAggregate.getItems().addAll(result.getPreAggregate());
      } else {
        preAggregate.getItems().add(item);
        // also push the alias forward in the chain
        OProjectionItem aggItem = new OProjectionItem(-1);
        aggItem.setExpression(new OExpression(item.getProjectionAlias()));
        aggregate.getItems().add(aggItem);
        postAggregate.getItems().add(aggItem);
      }
    }

    // bind split projections to the execution planner
    if (isSplitted) {
      info.preAggregateProjection = preAggregate;
      if (info.preAggregateProjection.getItems() == null
          || info.preAggregateProjection.getItems().size() == 0) {
        info.preAggregateProjection = null;
      }
      info.aggregateProjection = aggregate;
      if (info.aggregateProjection.getItems() == null
          || info.aggregateProjection.getItems().size() == 0) {
        info.aggregateProjection = null;
      }
      info.projection = postAggregate;

      addGroupByExpressionsToProjections(info);
    }
  }

  private static boolean isAggregate(OProjectionItem item) {
    return item.isAggregate();
  }

  private static OProjectionItem projectionFromAlias(OIdentifier oIdentifier) {
    OProjectionItem result = new OProjectionItem(-1);
    result.setExpression(new OExpression(oIdentifier));
    return result;
  }

  /**
   * if GROUP BY is performed on an expression that is not explicitly in the pre-aggregate
   * projections, then that expression has to be put in the pre-aggregate (only here, in subsequent
   * steps it's removed)
   */
  private static void addGroupByExpressionsToProjections(QueryPlanningInfo info) {
    if (info.groupBy == null
        || info.groupBy.getItems() == null
        || info.groupBy.getItems().size() == 0) {
      return;
    }
    OGroupBy newGroupBy = new OGroupBy(-1);
    int i = 0;
    for (OExpression exp : info.groupBy.getItems()) {
      if (exp.isAggregate()) {
        throw new OCommandExecutionException("Cannot group by an aggregate function");
      }
      boolean found = false;
      if (info.preAggregateProjection != null) {
        for (String alias : info.preAggregateProjection.getAllAliases()) {
          // if it's a simple identifier and it's the same as one of the projections in the query,
          // then the projection itself is used for GROUP BY without recalculating; in all the other
          // cases, it is evaluated separately
          if (alias.equals(exp.getDefaultAlias().getStringValue()) && exp.isBaseIdentifier()) {
            found = true;
            newGroupBy.getItems().add(exp);
            break;
          }
        }
      }
      if (!found) {
        OProjectionItem newItem = new OProjectionItem(-1);
        newItem.setExpression(exp);
        OIdentifier groupByAlias = new OIdentifier("_$$$GROUP_BY_ALIAS$$$_" + (i++));
        newItem.setAlias(groupByAlias);
        if (info.preAggregateProjection == null) {
          info.preAggregateProjection = new OProjection(-1);
        }
        if (info.preAggregateProjection.getItems() == null) {
          info.preAggregateProjection.setItems(new ArrayList<>());
        }
        info.preAggregateProjection.getItems().add(newItem);
        newGroupBy.getItems().add(new OExpression(groupByAlias));
      }

      info.groupBy = newGroupBy;
    }
  }

  /** translates subqueries to LET statements */
  private static void extractSubQueries(QueryPlanningInfo info) {
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

  private static void addGlobalLet(QueryPlanningInfo info, OIdentifier alias, OExpression exp) {
    if (info.globalLetClause == null) {
      info.globalLetClause = new OLetClause(-1);
    }
    OLetItem item = new OLetItem(-1);
    item.setVarName(alias);
    item.setExpression(exp);
    info.globalLetClause.addItem(item);
  }

  private static void addGlobalLet(QueryPlanningInfo info, OIdentifier alias, OStatement stm) {
    if (info.globalLetClause == null) {
      info.globalLetClause = new OLetClause(-1);
    }
    OLetItem item = new OLetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    info.globalLetClause.addItem(item);
  }

  private static void addGlobalLet(
      QueryPlanningInfo info, OIdentifier alias, OStatement stm, int pos) {
    if (info.globalLetClause == null) {
      info.globalLetClause = new OLetClause(-1);
    }
    OLetItem item = new OLetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    info.globalLetClause.getItems().add(pos, item);
  }

  private static void addRecordLevelLet(QueryPlanningInfo info, OIdentifier alias, OStatement stm) {
    if (info.perRecordLetClause == null) {
      info.perRecordLetClause = new OLetClause(-1);
    }
    OLetItem item = new OLetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    info.perRecordLetClause.addItem(item);
  }

  private static void addRecordLevelLet(
      QueryPlanningInfo info, OIdentifier alias, OStatement stm, int pos) {
    if (info.perRecordLetClause == null) {
      info.perRecordLetClause = new OLetClause(-1);
    }
    OLetItem item = new OLetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    info.perRecordLetClause.getItems().add(pos, item);
  }

  private void handleFetchFromTarger(
      OSelectExecutionPlan result,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {

    OFromItem target = info.target == null ? null : info.target.getItem();
    for (Map.Entry<String, OSelectExecutionPlan> shardedPlan :
        info.distributedFetchExecutionPlans.entrySet()) {
      if (target == null) {
        handleNoTarget(shardedPlan.getValue(), ctx, profilingEnabled);
      } else if (target.getIdentifier() != null) {
        String className = target.getIdentifier().getStringValue();
        if (className.startsWith("$")
            && !((ODatabaseDocumentInternal) ctx.getDatabase())
                .getMetadata()
                .getImmutableSchemaSnapshot()
                .existsClass(className)) {
          handleVariableAsTarget(shardedPlan.getValue(), info, ctx, profilingEnabled);
        } else {
          Set<String> filterClusters = info.serverToClusters.get(shardedPlan.getKey());

          OAndBlock ridRangeConditions = extractRidRanges(info.flattenedWhereClause, ctx);
          if (ridRangeConditions != null && !ridRangeConditions.isEmpty()) {
            info.ridRangeConditions = ridRangeConditions;
            filterClusters =
                filterClusters.stream()
                    .filter(
                        x -> clusterMatchesRidRange(x, ridRangeConditions, ctx.getDatabase(), ctx))
                    .collect(Collectors.toSet());
          }

          handleClassAsTarget(shardedPlan.getValue(), filterClusters, info, ctx, profilingEnabled);
        }
      } else if (target.getCluster() != null) {
        handleClustersAsTarget(
            shardedPlan.getValue(),
            info,
            Collections.singletonList(target.getCluster()),
            ctx,
            profilingEnabled);
      } else if (target.getClusterList() != null) {
        List<OCluster> allClusters = target.getClusterList().toListOfClusters();
        List<OCluster> clustersForShard = new ArrayList<>();
        for (OCluster cluster : allClusters) {
          String name = cluster.getClusterName();
          if (name == null) {
            name = ctx.getDatabase().getClusterNameById(cluster.getClusterNumber());
          }
          if (name != null && info.serverToClusters.get(shardedPlan.getKey()).contains(name)) {
            clustersForShard.add(cluster);
          }
        }
        handleClustersAsTarget(
            shardedPlan.getValue(), info, clustersForShard, ctx, profilingEnabled);
      } else if (target.getStatement() != null) {
        handleSubqueryAsTarget(
            shardedPlan.getValue(), target.getStatement(), ctx, profilingEnabled);
      } else if (target.getFunctionCall() != null) {
        //        handleFunctionCallAsTarget(result, target.getFunctionCall(), ctx);//TODO
        throw new OCommandExecutionException("function call as target is not supported yet");
      } else if (target.getInputParam() != null) {
        handleInputParamAsTarget(
            shardedPlan.getValue(),
            info.serverToClusters.get(shardedPlan.getKey()),
            info,
            target.getInputParam(),
            ctx,
            profilingEnabled);
      } else if (target.getInputParams() != null && target.getInputParams().size() > 0) {
        List<OInternalExecutionPlan> plans = new ArrayList<>();
        for (OInputParameter param : target.getInputParams()) {
          OSelectExecutionPlan subPlan = new OSelectExecutionPlan(ctx);
          handleInputParamAsTarget(
              subPlan,
              info.serverToClusters.get(shardedPlan.getKey()),
              info,
              param,
              ctx,
              profilingEnabled);
          plans.add(subPlan);
        }
        shardedPlan.getValue().chain(new ParallelExecStep(plans, ctx, profilingEnabled));
      } else if (target.getIndex() != null) {
        handleIndexAsTarget(
            shardedPlan.getValue(), info, target.getIndex(), null, ctx, profilingEnabled);
        if (info.serverToClusters.size() > 1) {
          shardedPlan
              .getValue()
              .chain(
                  new FilterByClustersStep(
                      info.serverToClusters.get(shardedPlan.getKey()), ctx, profilingEnabled));
        }
      } else if (target.getMetadata() != null) {
        handleMetadataAsTarget(shardedPlan.getValue(), target.getMetadata(), ctx, profilingEnabled);
      } else if (target.getRids() != null && target.getRids().size() > 0) {
        Set<String> filterClusters = info.serverToClusters.get(shardedPlan.getKey());
        List<ORid> rids = new ArrayList<>();
        for (ORid rid : target.getRids()) {
          if (filterClusters == null || isFromClusters(rid, filterClusters, ctx.getDatabase())) {
            rids.add(rid);
          }
        }
        if (rids.size() > 0) {
          handleRidsAsTarget(shardedPlan.getValue(), rids, ctx, profilingEnabled);
        } else {
          result.chain(new EmptyStep(ctx, profilingEnabled)); // nothing to return
        }
      } else {
        throw new UnsupportedOperationException();
      }
    }
  }

  private void handleVariableAsTarget(
      OSelectExecutionPlan plan,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {
    plan.chain(
        new FetchFromVariableStep(
            info.target.getItem().getIdentifier().getStringValue(), ctx, profilingEnabled));
  }

  private boolean clusterMatchesRidRange(
      String clusterName, OAndBlock ridRangeConditions, ODatabase database, OCommandContext ctx) {
    int thisClusterId = database.getClusterIdByName(clusterName);
    for (OBooleanExpression ridRangeCondition : ridRangeConditions.getSubBlocks()) {
      if (ridRangeCondition instanceof OBinaryCondition) {
        OBinaryCompareOperator operator = ((OBinaryCondition) ridRangeCondition).getOperator();
        ORID conditionRid;

        Object obj;
        if (((OBinaryCondition) ridRangeCondition).getRight().getRid() != null) {
          obj =
              ((OBinaryCondition) ridRangeCondition)
                  .getRight()
                  .getRid()
                  .toRecordId((OResult) null, ctx);
        } else {
          obj = ((OBinaryCondition) ridRangeCondition).getRight().execute((OResult) null, ctx);
        }

        conditionRid = ((OIdentifiable) obj).getIdentity();

        if (conditionRid != null) {
          int conditionClusterId = conditionRid.getClusterId();
          if (operator instanceof OGtOperator || operator instanceof OGeOperator) {
            if (thisClusterId < conditionClusterId) {
              return false;
            }
          } else if (operator instanceof OLtOperator || operator instanceof OLeOperator) {
            if (thisClusterId > conditionClusterId) {
              return false;
            }
          }
        }
      }
    }
    return true;
  }

  private OAndBlock extractRidRanges(List<OAndBlock> flattenedWhereClause, OCommandContext ctx) {
    OAndBlock result = new OAndBlock(-1);

    if (flattenedWhereClause == null || flattenedWhereClause.size() != 1) {
      return result;
    }
    // TODO optimization: merge multiple conditions

    for (OBooleanExpression booleanExpression : flattenedWhereClause.get(0).getSubBlocks()) {
      if (isRidRange(booleanExpression, ctx)) {
        result.getSubBlocks().add(booleanExpression.copy());
      }
    }

    return result;
  }

  private boolean isRidRange(OBooleanExpression booleanExpression, OCommandContext ctx) {
    if (booleanExpression instanceof OBinaryCondition) {
      OBinaryCondition cond = ((OBinaryCondition) booleanExpression);
      OBinaryCompareOperator operator = cond.getOperator();
      if (operator.isRangeOperator() && cond.getLeft().toString().equalsIgnoreCase("@rid")) {
        Object obj;
        if (cond.getRight().getRid() != null) {
          obj = cond.getRight().getRid().toRecordId((OResult) null, ctx);
        } else {
          obj = cond.getRight().execute((OResult) null, ctx);
        }
        return obj instanceof OIdentifiable;
      }
    }
    return false;
  }

  private void handleInputParamAsTarget(
      OSelectExecutionPlan result,
      Set<String> filterClusters,
      QueryPlanningInfo info,
      OInputParameter inputParam,
      OCommandContext ctx,
      boolean profilingEnabled) {
    Object paramValue = inputParam.getValue(ctx.getInputParameters());
    if (paramValue == null) {
      result.chain(new EmptyStep(ctx, profilingEnabled)); // nothing to return
    } else if (paramValue instanceof OClass) {
      OFromClause from = new OFromClause(-1);
      OFromItem item = new OFromItem(-1);
      from.setItem(item);
      item.setIdentifier(new OIdentifier(((OClass) paramValue).getName()));
      handleClassAsTarget(result, filterClusters, from, info, ctx, profilingEnabled);
    } else if (paramValue instanceof String) {
      // strings are treated as classes
      OFromClause from = new OFromClause(-1);
      OFromItem item = new OFromItem(-1);
      from.setItem(item);
      item.setIdentifier(new OIdentifier((String) paramValue));
      handleClassAsTarget(result, filterClusters, from, info, ctx, profilingEnabled);
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

      if (filterClusters == null || isFromClusters(rid, filterClusters, ctx.getDatabase())) {
        handleRidsAsTarget(result, Collections.singletonList(rid), ctx, profilingEnabled);
      } else {
        result.chain(new EmptyStep(ctx, profilingEnabled)); // nothing to return
      }

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
        if (filterClusters == null || isFromClusters(rid, filterClusters, ctx.getDatabase())) {
          rids.add(rid);
        }
      }
      if (rids.size() > 0) {
        handleRidsAsTarget(result, rids, ctx, profilingEnabled);
      } else {
        result.chain(new EmptyStep(ctx, profilingEnabled)); // nothing to return
      }
    } else {
      throw new OCommandExecutionException("Invalid target: " + paramValue);
    }
  }

  /**
   * checks if this RID is from one of these clusters
   *
   * @param rid
   * @param filterClusters
   * @param database
   * @return
   */
  private boolean isFromClusters(ORid rid, Set<String> filterClusters, ODatabase database) {
    if (filterClusters == null) {
      throw new IllegalArgumentException();
    }
    String clusterName = database.getClusterNameById(rid.getCluster().getValue().intValue());
    return filterClusters.contains(clusterName);
  }

  private void handleNoTarget(
      OSelectExecutionPlan result, OCommandContext ctx, boolean profilingEnabled) {
    result.chain(new EmptyDataGeneratorStep(1, ctx, profilingEnabled));
  }

  private void handleIndexAsTarget(
      OSelectExecutionPlan result,
      QueryPlanningInfo info,
      OIndexIdentifier indexIdentifier,
      Set<String> filterClusters,
      OCommandContext ctx,
      boolean profilingEnabled) {

    OIndexAbstract.manualIndexesWarning();
    String indexName = indexIdentifier.getIndexName();
    final ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
    OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, indexName);
    if (index == null) {
      throw new OCommandExecutionException("Index not found: " + indexName);
    }

    int[] filterClusterIds = null;
    if (filterClusters != null) {
      filterClusterIds = database.getClustersIds(filterClusters);
    }

    switch (indexIdentifier.getType()) {
      case INDEX:
        OBooleanExpression keyCondition = null;
        OBooleanExpression ridCondition = null;
        if (info.flattenedWhereClause == null || info.flattenedWhereClause.size() == 0) {
          if (!index.supportsOrderedIterations()) {
            throw new OCommandExecutionException(
                "Index " + indexName + " does not allow iteration without a condition");
          }
        } else if (info.flattenedWhereClause.size() > 1) {
          throw new OCommandExecutionException(
              "Index queries with this kind of condition are not supported yet: "
                  + info.whereClause);
        } else {
          OAndBlock andBlock = info.flattenedWhereClause.get(0);
          if (andBlock.getSubBlocks().size() == 1) {

            info.whereClause =
                null; // The WHERE clause won't be used anymore, the index does all the filtering
            info.flattenedWhereClause = null;
            keyCondition = getKeyCondition(andBlock);
            if (keyCondition == null) {
              throw new OCommandExecutionException(
                  "Index queries with this kind of condition are not supported yet: "
                      + info.whereClause);
            }
          } else if (andBlock.getSubBlocks().size() == 2) {
            info.whereClause =
                null; // The WHERE clause won't be used anymore, the index does all the filtering
            info.flattenedWhereClause = null;
            keyCondition = getKeyCondition(andBlock);
            ridCondition = getRidCondition(andBlock);
            if (keyCondition == null || ridCondition == null) {
              throw new OCommandExecutionException(
                  "Index queries with this kind of condition are not supported yet: "
                      + info.whereClause);
            }
          } else {
            throw new OCommandExecutionException(
                "Index queries with this kind of condition are not supported yet: "
                    + info.whereClause);
          }
        }
        result.chain(new FetchFromIndexStep(index, keyCondition, null, ctx, profilingEnabled));
        if (ridCondition != null) {
          OWhereClause where = new OWhereClause(-1);
          where.setBaseExpression(ridCondition);
          result.chain(
              new FilterStep(
                  where,
                  ctx,
                  this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                  profilingEnabled));
        }
        break;
      case VALUES:
      case VALUESASC:
        if (!index.supportsOrderedIterations()) {
          throw new OCommandExecutionException(
              "Index " + indexName + " does not allow iteration on values");
        }
        result.chain(new FetchFromIndexValuesStep(index, true, ctx, profilingEnabled));
        result.chain(new GetValueFromIndexEntryStep(ctx, filterClusterIds, profilingEnabled));
        break;
      case VALUESDESC:
        if (!index.supportsOrderedIterations()) {
          throw new OCommandExecutionException(
              "Index " + indexName + " does not allow iteration on values");
        }
        result.chain(new FetchFromIndexValuesStep(index, false, ctx, profilingEnabled));
        result.chain(new GetValueFromIndexEntryStep(ctx, filterClusterIds, profilingEnabled));
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

  private void handleMetadataAsTarget(
      OSelectExecutionPlan plan,
      OMetadataIdentifier metadata,
      OCommandContext ctx,
      boolean profilingEnabled) {
    ODatabaseInternal db = (ODatabaseInternal) ctx.getDatabase();
    String schemaRecordIdAsString = null;
    if (metadata.getName().equalsIgnoreCase(OCommandExecutorSQLAbstract.METADATA_SCHEMA)) {
      schemaRecordIdAsString = db.getStorageInfo().getConfiguration().getSchemaRecordId();
      ORecordId schemaRid = new ORecordId(schemaRecordIdAsString);
      plan.chain(new FetchFromRidsStep(Collections.singleton(schemaRid), ctx, profilingEnabled));
    } else if (metadata.getName().equalsIgnoreCase(OCommandExecutorSQLAbstract.METADATA_INDEXMGR)) {
      schemaRecordIdAsString = db.getStorageInfo().getConfiguration().getIndexMgrRecordId();
      ORecordId schemaRid = new ORecordId(schemaRecordIdAsString);
      plan.chain(new FetchFromRidsStep(Collections.singleton(schemaRid), ctx, profilingEnabled));
    } else if (metadata.getName().equalsIgnoreCase(OCommandExecutorSQLAbstract.METADATA_STORAGE)) {
      plan.chain(new FetchFromStorageMetadataStep(ctx, profilingEnabled));
    } else if (metadata.getName().equalsIgnoreCase(OCommandExecutorSQLAbstract.METADATA_DATABASE)) {
      plan.chain(new FetchFromDatabaseMetadataStep(ctx, profilingEnabled));
    } else {
      throw new UnsupportedOperationException("Invalid metadata: " + metadata.getName());
    }
  }

  private void handleRidsAsTarget(
      OSelectExecutionPlan plan, List<ORid> rids, OCommandContext ctx, boolean profilingEnabled) {
    List<ORecordId> actualRids = new ArrayList<>();
    for (ORid rid : rids) {
      actualRids.add(rid.toRecordId((OResult) null, ctx));
    }
    plan.chain(new FetchFromRidsStep(actualRids, ctx, profilingEnabled));
  }

  private static void handleExpand(
      OSelectExecutionPlan result,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {
    if (info.expand) {
      result.chain(new ExpandStep(ctx, profilingEnabled));
    }
  }

  private void handleGlobalLet(
      OSelectExecutionPlan result,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {
    if (info.globalLetClause != null) {
      List<OLetItem> items = info.globalLetClause.getItems();
      items = sortLet(items, this.statement.getLetClause());
      List<String> scriptVars = new ArrayList<>();
      for (OLetItem item : items) {
        if (item.getExpression() != null) {
          result.chain(
              new GlobalLetExpressionStep(
                  item.getVarName(), item.getExpression(), ctx, profilingEnabled));
        } else {
          result.chain(
              new GlobalLetQueryStep(
                  item.getVarName(), item.getQuery(), ctx, profilingEnabled, scriptVars));
        }
        scriptVars.add(item.getVarName().getStringValue());
        info.globalLetPresent = true;
      }
    }
  }

  private void handleLet(
      OSelectExecutionPlan plan,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {
    // this could be invoked multiple times
    // so it can be optimized
    // checking whether the execution plan already contains some LET steps
    // and in case skip
    if (info.perRecordLetClause != null) {
      List<OLetItem> items = info.perRecordLetClause.getItems();
      items = sortLet(items, this.statement.getLetClause());
      if (plan.steps.size() > 0 || info.distributedPlanCreated) {
        for (OLetItem item : items) {
          if (item.getExpression() != null) {
            plan.chain(
                new LetExpressionStep(
                    item.getVarName(), item.getExpression(), ctx, profilingEnabled));
          } else {
            plan.chain(new LetQueryStep(item.getVarName(), item.getQuery(), ctx, profilingEnabled));
          }
        }
      } else {
        for (OSelectExecutionPlan shardedPlan : info.distributedFetchExecutionPlans.values()) {
          for (OLetItem item : items) {
            if (item.getExpression() != null) {
              shardedPlan.chain(
                  new LetExpressionStep(
                      item.getVarName().copy(),
                      item.getExpression().copy(),
                      ctx,
                      profilingEnabled));
            } else {
              shardedPlan.chain(
                  new LetQueryStep(
                      item.getVarName().copy(), item.getQuery().copy(), ctx, profilingEnabled));
            }
          }
        }
      }
    }
  }

  private List<OLetItem> sortLet(List<OLetItem> items, OLetClause letClause) {
    if (letClause == null) {
      return items;
    }
    List<OLetItem> i = new ArrayList<>();
    i.addAll(items);
    ArrayList<OLetItem> result = new ArrayList<>();
    for (OLetItem item : letClause.getItems()) {
      String var = item.getVarName().getStringValue();
      Iterator<OLetItem> iterator = i.iterator();
      while (iterator.hasNext()) {
        OLetItem x = iterator.next();
        if (x.getVarName().getStringValue().equals(var)) {
          iterator.remove();
          result.add(x);
          break;
        }
      }
    }
    for (OLetItem item : i) {

      result.add(item);
    }
    return result;
  }

  private void handleWhere(
      OSelectExecutionPlan plan,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {
    if (info.whereClause != null) {
      if (info.distributedPlanCreated) {
        plan.chain(
            new FilterStep(
                info.whereClause,
                ctx,
                this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                profilingEnabled));
      } else {
        for (OSelectExecutionPlan shardedPlan : info.distributedFetchExecutionPlans.values()) {
          shardedPlan.chain(
              new FilterStep(
                  info.whereClause.copy(),
                  ctx,
                  this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                  profilingEnabled));
        }
      }
    }
  }

  public static void handleOrderBy(
      OSelectExecutionPlan plan,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {
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
    if (!info.orderApplied
        && info.orderBy != null
        && info.orderBy.getItems() != null
        && info.orderBy.getItems().size() > 0) {
      plan.chain(
          new OrderByStep(
              info.orderBy,
              maxResults,
              ctx,
              info.timeout != null ? info.timeout.getVal().longValue() : -1,
              profilingEnabled));
      if (info.projectionAfterOrderBy != null) {
        plan.chain(
            new ProjectionCalculationStep(info.projectionAfterOrderBy, ctx, profilingEnabled));
      }
    }
  }

  /**
   * @param plan the execution plan where to add the fetch step
   * @param filterClusters clusters of interest (all the others have to be excluded from the result)
   * @param info
   * @param ctx
   * @param profilingEnabled
   */
  private void handleClassAsTarget(
      OSelectExecutionPlan plan,
      Set<String> filterClusters,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {
    handleClassAsTarget(plan, filterClusters, info.target, info, ctx, profilingEnabled);
  }

  private void handleClassAsTarget(
      OSelectExecutionPlan plan,
      Set<String> filterClusters,
      OFromClause from,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {
    OIdentifier identifier = from.getItem().getIdentifier();
    if (handleClassAsTargetWithIndexedFunction(
        plan, filterClusters, identifier, info, ctx, profilingEnabled)) {
      plan.chain(new FilterByClassStep(identifier, ctx, profilingEnabled));
      return;
    }

    if (handleClassAsTargetWithIndex(
        plan, identifier, filterClusters, info, ctx, profilingEnabled)) {
      plan.chain(new FilterByClassStep(identifier, ctx, profilingEnabled));
      return;
    }

    if (info.orderBy != null
        && handleClassWithIndexForSortOnly(
            plan, identifier, filterClusters, info, ctx, profilingEnabled)) {
      plan.chain(new FilterByClassStep(identifier, ctx, profilingEnabled));
      return;
    }

    Boolean orderByRidAsc = null; // null: no order. true: asc, false:desc
    if (isOrderByRidAsc(info)) {
      orderByRidAsc = true;
    } else if (isOrderByRidDesc(info)) {
      orderByRidAsc = false;
    }
    String className = identifier.getStringValue();
    OSchema schema = getSchemaFromContext(ctx);

    AbstractExecutionStep fetcher;
    if (schema.getClass(className) != null) {
      fetcher =
          new FetchFromClassExecutionStep(
              className, filterClusters, info, ctx, orderByRidAsc, profilingEnabled);
    } else if (schema.getView(className) != null) {
      fetcher =
          new FetchFromViewExecutionStep(
              className, filterClusters, info, ctx, orderByRidAsc, profilingEnabled);
    } else {
      throw new OCommandExecutionException("Class or View not present in the schema: " + className);
    }

    if (orderByRidAsc != null && info.serverToClusters.size() == 1) {
      info.orderApplied = true;
    }
    plan.chain(fetcher);
  }

  private boolean handleClassAsTargetWithIndexedFunction(
      OSelectExecutionPlan plan,
      Set<String> filterClusters,
      OIdentifier queryTarget,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {
    if (queryTarget == null) {
      return false;
    }
    OSchema schema = getSchemaFromContext(ctx);
    OClass clazz = schema.getClass(queryTarget.getStringValue());
    if (clazz == null) {
      clazz = schema.getView(queryTarget.getStringValue());
      if (clazz == null) {
        throw new OCommandExecutionException("Class not found: " + queryTarget);
      }
    }
    if (info.flattenedWhereClause == null || info.flattenedWhereClause.size() == 0) {
      return false;
    }

    List<OInternalExecutionPlan> resultSubPlans = new ArrayList<>();

    boolean indexedFunctionsFound = false;

    for (OAndBlock block : info.flattenedWhereClause) {
      List<OBinaryCondition> indexedFunctionConditions =
          block.getIndexedFunctionConditions(clazz, (ODatabaseDocumentInternal) ctx.getDatabase());

      indexedFunctionConditions =
          filterIndexedFunctionsWithoutIndex(indexedFunctionConditions, info.target, ctx);

      if (indexedFunctionConditions == null || indexedFunctionConditions.size() == 0) {
        IndexSearchDescriptor bestIndex = findBestIndexFor(ctx, clazz.getIndexes(), block, clazz);
        if (bestIndex != null) {

          FetchFromIndexStep step =
              new FetchFromIndexStep(
                  bestIndex.idx,
                  bestIndex.keyCondition,
                  bestIndex.additionalRangeCondition,
                  true,
                  ctx,
                  profilingEnabled);

          OSelectExecutionPlan subPlan = new OSelectExecutionPlan(ctx);
          subPlan.chain(step);
          int[] filterClusterIds = null;
          if (filterClusters != null) {
            filterClusterIds =
                ((ODatabaseDocumentInternal) ctx.getDatabase()).getClustersIds(filterClusters);
          }
          subPlan.chain(new GetValueFromIndexEntryStep(ctx, filterClusterIds, profilingEnabled));
          if (requiresMultipleIndexLookups(bestIndex.keyCondition)
              || duplicateResultsForRecord(bestIndex)) {
            subPlan.chain(new DistinctExecutionStep(ctx, profilingEnabled));
          }
          if (!block.getSubBlocks().isEmpty()) {
            if ((info.perRecordLetClause != null && refersToLet(block.getSubBlocks()))) {
              handleLet(subPlan, info, ctx, profilingEnabled);
            }
            subPlan.chain(
                new FilterStep(
                    createWhereFrom(block),
                    ctx,
                    this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                    profilingEnabled));
          }
          resultSubPlans.add(subPlan);
        } else {
          FetchFromClassExecutionStep step;
          if (clazz instanceof OView) {
            step =
                new FetchFromViewExecutionStep(
                    clazz.getName(), filterClusters, info, ctx, true, profilingEnabled);
          } else {
            step =
                new FetchFromClassExecutionStep(
                    clazz.getName(), filterClusters, ctx, true, profilingEnabled);
          }
          OSelectExecutionPlan subPlan = new OSelectExecutionPlan(ctx);
          subPlan.chain(step);
          if (!block.getSubBlocks().isEmpty()) {
            if ((info.perRecordLetClause != null && refersToLet(block.getSubBlocks()))) {
              handleLet(subPlan, info, ctx, profilingEnabled);
            }
            subPlan.chain(
                new FilterStep(
                    createWhereFrom(block),
                    ctx,
                    this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                    profilingEnabled));
          }
          resultSubPlans.add(subPlan);
        }
      } else {
        OBinaryCondition blockCandidateFunction = null;
        for (OBinaryCondition cond : indexedFunctionConditions) {
          if (!cond.allowsIndexedFunctionExecutionOnTarget(info.target, ctx)) {
            if (!cond.canExecuteIndexedFunctionWithoutIndex(info.target, ctx)) {
              throw new OCommandExecutionException(
                  "Cannot execute " + block + " on " + queryTarget);
            }
          }
          if (blockCandidateFunction == null) {
            blockCandidateFunction = cond;
          } else {
            boolean thisAllowsNoIndex =
                cond.canExecuteIndexedFunctionWithoutIndex(info.target, ctx);
            boolean prevAllowsNoIndex =
                blockCandidateFunction.canExecuteIndexedFunctionWithoutIndex(info.target, ctx);
            if (!thisAllowsNoIndex && !prevAllowsNoIndex) {
              // none of the functions allow execution without index, so cannot choose one
              throw new OCommandExecutionException(
                  "Cannot choose indexed function between "
                      + cond
                      + " and "
                      + blockCandidateFunction
                      + ". Both require indexed execution");
            } else if (thisAllowsNoIndex && prevAllowsNoIndex) {
              // both can be calculated without index, choose the best one for index execution
              long thisEstimate = cond.estimateIndexed(info.target, ctx);
              long lastEstimate = blockCandidateFunction.estimateIndexed(info.target, ctx);
              if (thisEstimate > -1 && thisEstimate < lastEstimate) {
                blockCandidateFunction = cond;
              }
            } else if (prevAllowsNoIndex) {
              // choose current condition, because the other one can be calculated without index
              blockCandidateFunction = cond;
            }
          }
        }

        FetchFromIndexedFunctionStep step =
            new FetchFromIndexedFunctionStep(
                blockCandidateFunction, info.target, ctx, profilingEnabled);
        if (!blockCandidateFunction.executeIndexedFunctionAfterIndexSearch(info.target, ctx)) {
          block = block.copy();
          block.getSubBlocks().remove(blockCandidateFunction);
        }
        if (info.flattenedWhereClause.size() == 1) {
          plan.chain(step);
          plan.chain(new FilterByClustersStep(filterClusters, ctx, profilingEnabled));
          if (!block.getSubBlocks().isEmpty()) {
            if ((info.perRecordLetClause != null && refersToLet(block.getSubBlocks()))) {
              handleLet(plan, info, ctx, profilingEnabled);
            }
            plan.chain(
                new FilterStep(
                    createWhereFrom(block),
                    ctx,
                    this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                    profilingEnabled));
          }
        } else {
          OSelectExecutionPlan subPlan = new OSelectExecutionPlan(ctx);
          subPlan.chain(step);
          if (!block.getSubBlocks().isEmpty()) {
            subPlan.chain(
                new FilterStep(
                    createWhereFrom(block),
                    ctx,
                    this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                    profilingEnabled));
          }
          resultSubPlans.add(subPlan);
        }
        indexedFunctionsFound = true;
      }
    }

    if (indexedFunctionsFound) {
      if (resultSubPlans.size()
          > 1) { // if resultSubPlans.size() == 1 the step was already chained (see above)
        plan.chain(new ParallelExecStep(resultSubPlans, ctx, profilingEnabled));
        plan.chain(new FilterByClustersStep(filterClusters, ctx, profilingEnabled));
        plan.chain(new DistinctExecutionStep(ctx, profilingEnabled));
      }
      // WHERE condition already applied
      info.whereClause = null;
      info.flattenedWhereClause = null;
      return true;
    } else {
      return false;
    }
  }

  private boolean duplicateResultsForRecord(IndexSearchDescriptor bestIndex) {
    if (bestIndex.idx.getDefinition() instanceof OCompositeIndexDefinition) {
      if (((OCompositeIndexDefinition) bestIndex.idx.getDefinition()).getMultiValueDefinition()
          != null) {
        return true;
      }
    }
    return false;
  }

  private boolean refersToLet(List<OBooleanExpression> subBlocks) {
    if (subBlocks == null) {
      return false;
    }
    for (OBooleanExpression exp : subBlocks) {
      if (exp.toString().startsWith("$")) {
        return true;
      }
    }
    return false;
  }

  private List<OBinaryCondition> filterIndexedFunctionsWithoutIndex(
      List<OBinaryCondition> indexedFunctionConditions,
      OFromClause fromClause,
      OCommandContext ctx) {
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
   * @param ctx the current context
   * @return true if it succeeded to use an index to sort, false otherwise.
   */
  private boolean handleClassWithIndexForSortOnly(
      OSelectExecutionPlan plan,
      OIdentifier queryTarget,
      Set<String> filterClusters,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {
    OSchema schema = getSchemaFromContext(ctx);
    OClass clazz = schema.getClass(queryTarget.getStringValue());
    if (clazz == null) {
      clazz = schema.getView(queryTarget.getStringValue());
      if (clazz == null) {
        throw new OCommandExecutionException("Class not found: " + queryTarget);
      }
    }

    for (OIndex idx :
        clazz.getIndexes().stream()
            .filter(i -> i.supportsOrderedIterations())
            .filter(i -> i.getDefinition() != null)
            .collect(Collectors.toList())) {
      List<String> indexFields = idx.getDefinition().getFields();
      if (indexFields.size() < info.orderBy.getItems().size()) {
        continue;
      }
      boolean indexFound = true;
      String orderType = null;
      for (int i = 0; i < info.orderBy.getItems().size(); i++) {
        OOrderByItem orderItem = info.orderBy.getItems().get(i);
        if (orderItem.getCollate() != null) {
          return false;
        }
        String indexField = indexFields.get(i);
        if (i == 0) {
          orderType = orderItem.getType();
        } else {
          if (orderType == null || !orderType.equals(orderItem.getType())) {
            indexFound = false;
            break; // ASC/DESC interleaved, cannot be used with index.
          }
        }
        if (!(indexField.equals(orderItem.getAlias())
            || isInOriginalProjection(indexField, orderItem.getAlias()))) {
          indexFound = false;
          break;
        }
      }
      if (indexFound && orderType != null) {
        plan.chain(
            new FetchFromIndexValuesStep(
                idx, orderType.equals(OOrderByItem.ASC), ctx, profilingEnabled));
        int[] filterClusterIds = null;
        if (filterClusters != null) {
          filterClusterIds =
              ((ODatabaseDocumentInternal) ctx.getDatabase()).getClustersIds(filterClusters);
        }
        plan.chain(new GetValueFromIndexEntryStep(ctx, filterClusterIds, profilingEnabled));
        if (info.serverToClusters.size() == 1) {
          info.orderApplied = true;
        }
        return true;
      }
    }
    return false;
  }

  private boolean isInOriginalProjection(String indexField, String alias) {
    if (info.projection == null) {
      return false;
    }
    if (info.projection.getItems() == null) {
      return false;
    }
    return info.projection.getItems().stream()
        .filter(proj -> proj.getExpression().toString().equals(indexField))
        .filter(proj -> proj.getAlias() != null)
        .anyMatch(proj -> proj.getAlias().getStringValue().equals(alias));
  }

  private boolean handleClassAsTargetWithIndex(
      OSelectExecutionPlan plan,
      OIdentifier targetClass,
      Set<String> filterClusters,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {

    List<OExecutionStepInternal> result =
        handleClassAsTargetWithIndex(
            targetClass.getStringValue(), filterClusters, info, ctx, profilingEnabled);
    if (result != null) {
      result.stream().forEach(x -> plan.chain(x));
      info.whereClause = null;
      info.flattenedWhereClause = null;
      return true;
    }
    OSchema schema = getSchemaFromContext(ctx);
    OClass clazz = schema.getClass(targetClass.getStringValue());
    if (clazz == null) {
      clazz = schema.getView(targetClass.getStringValue());
      if (clazz == null) {
        throw new OCommandExecutionException("Class not found: " + targetClass);
      }
    }
    if (clazz.count(false) != 0 || clazz.getSubclasses().size() == 0 || isDiamondHierarchy(clazz)) {
      return false;
    }
    // try subclasses

    Collection<OClass> subclasses = clazz.getSubclasses();

    List<OInternalExecutionPlan> subclassPlans = new ArrayList<>();
    for (OClass subClass : subclasses) {
      List<OExecutionStepInternal> subSteps =
          handleClassAsTargetWithIndexRecursive(
              subClass.getName(), filterClusters, info, ctx, profilingEnabled);
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

  private List<OExecutionStepInternal> handleClassAsTargetWithIndexRecursive(
      String targetClass,
      Set<String> filterClusters,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {
    List<OExecutionStepInternal> result =
        handleClassAsTargetWithIndex(targetClass, filterClusters, info, ctx, profilingEnabled);
    if (result == null) {
      result = new ArrayList<>();
      OClass clazz = getSchemaFromContext(ctx).getClass(targetClass);
      if (clazz == null) {
        clazz = getSchemaFromContext(ctx).getView(targetClass);
      }
      if (clazz == null) {
        throw new OCommandExecutionException("Cannot find class " + targetClass);
      }
      if (clazz.count(false) != 0
          || clazz.getSubclasses().size() == 0
          || isDiamondHierarchy(clazz)) {
        return null;
      }

      Collection<OClass> subclasses = clazz.getSubclasses();

      List<OInternalExecutionPlan> subclassPlans = new ArrayList<>();
      for (OClass subClass : subclasses) {
        List<OExecutionStepInternal> subSteps =
            handleClassAsTargetWithIndexRecursive(
                subClass.getName(), filterClusters, info, ctx, profilingEnabled);
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

  private List<OExecutionStepInternal> handleClassAsTargetWithIndex(
      String targetClass,
      Set<String> filterClusters,
      QueryPlanningInfo info,
      OCommandContext ctx,
      boolean profilingEnabled) {
    if (info.flattenedWhereClause == null || info.flattenedWhereClause.size() == 0) {
      return null;
    }

    OClass clazz = getSchemaFromContext(ctx).getClass(targetClass);
    if (clazz == null) {
      clazz = getSchemaFromContext(ctx).getView(targetClass);
    }
    if (clazz == null) {
      throw new OCommandExecutionException("Cannot find class " + targetClass);
    }

    Set<OIndex> indexes = clazz.getIndexes();

    final OClass c = clazz;
    List<IndexSearchDescriptor> indexSearchDescriptors =
        info.flattenedWhereClause.stream()
            .map(x -> findBestIndexFor(ctx, indexes, x, c))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    if (indexSearchDescriptors.size() != info.flattenedWhereClause.size()) {
      return null; // some blocks could not be managed with an index
    }

    List<OExecutionStepInternal> result = null;
    List<IndexSearchDescriptor> optimumIndexSearchDescriptors =
        commonFactor(indexSearchDescriptors);

    if (indexSearchDescriptors.size() == 1) {
      IndexSearchDescriptor desc = indexSearchDescriptors.get(0);
      result = new ArrayList<>();
      Boolean orderAsc = getOrderDirection(info);
      result.add(
          new FetchFromIndexStep(
              desc.idx,
              desc.keyCondition,
              desc.additionalRangeCondition,
              !Boolean.FALSE.equals(orderAsc),
              ctx,
              profilingEnabled));
      int[] filterClusterIds = null;
      if (filterClusters != null) {
        filterClusterIds =
            ((ODatabaseDocumentInternal) ctx.getDatabase()).getClustersIds(filterClusters);
      }
      result.add(new GetValueFromIndexEntryStep(ctx, filterClusterIds, profilingEnabled));
      if (requiresMultipleIndexLookups(desc.keyCondition) || duplicateResultsForRecord(desc)) {
        result.add(new DistinctExecutionStep(ctx, profilingEnabled));
      }
      if (orderAsc != null
          && info.orderBy != null
          && fullySorted(info.orderBy, desc.keyCondition, desc.idx)
          && info.serverToClusters.size() == 1) {
        info.orderApplied = true;
      }
      if (desc.remainingCondition != null && !desc.remainingCondition.isEmpty()) {
        if ((info.perRecordLetClause != null
            && refersToLet(Collections.singletonList(desc.remainingCondition)))) {
          OSelectExecutionPlan stubPlan = new OSelectExecutionPlan(ctx);
          boolean prevCreatedDist = info.distributedPlanCreated;
          info.distributedPlanCreated = true; // little hack, check this!!!
          handleLet(stubPlan, info, ctx, profilingEnabled);
          for (OExecutionStep step : stubPlan.getSteps()) {
            result.add((OExecutionStepInternal) step);
          }
          info.distributedPlanCreated = prevCreatedDist;
        }
        result.add(
            new FilterStep(
                createWhereFrom(desc.remainingCondition),
                ctx,
                this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                profilingEnabled));
      }
    } else {
      result = new ArrayList<>();
      result.add(
          createParallelIndexFetch(
              optimumIndexSearchDescriptors, filterClusters, ctx, profilingEnabled));
      if (optimumIndexSearchDescriptors.size() > 1) {
        result.add(new DistinctExecutionStep(ctx, profilingEnabled));
      }
    }
    return result;
  }

  private static OSchema getSchemaFromContext(OCommandContext ctx) {
    return ((OMetadataInternal) ctx.getDatabase().getMetadata()).getImmutableSchemaSnapshot();
  }

  private boolean fullySorted(OOrderBy orderBy, OAndBlock conditions, OIndex idx) {
    if (orderBy.getItems().stream().anyMatch(x -> x.getCollate() != null)) {
      return false;
    }

    if (!idx.supportsOrderedIterations()) return false;

    List<String> orderItems = new ArrayList<>();
    String order = null;

    for (OOrderByItem item : orderBy.getItems()) {
      if (order == null) {
        order = item.getType();
      } else if (!order.equals(item.getType())) {
        return false;
      }
      orderItems.add(item.getAlias() != null ? item.getAlias() : item.getRecordAttr());
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
        return true; // nothing to sort, the conditions completely overlap the ORDER BY
      }
      if (s.equals(orderItems.get(0))) {
        orderItems.remove(0);
        overlapping = true; // start overlapping
      } else if (overlapping) {
        return false; // overlapping, but next order item does not match...
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
    return result == null || result.equals(OOrderByItem.ASC);
  }

  private OExecutionStepInternal createParallelIndexFetch(
      List<IndexSearchDescriptor> indexSearchDescriptors,
      Set<String> filterClusters,
      OCommandContext ctx,
      boolean profilingEnabled) {
    List<OInternalExecutionPlan> subPlans = new ArrayList<>();
    for (IndexSearchDescriptor desc : indexSearchDescriptors) {
      OSelectExecutionPlan subPlan = new OSelectExecutionPlan(ctx);
      subPlan.chain(
          new FetchFromIndexStep(
              desc.idx, desc.keyCondition, desc.additionalRangeCondition, ctx, profilingEnabled));
      int[] filterClusterIds = null;
      if (filterClusters != null) {
        filterClusterIds =
            ((ODatabaseDocumentInternal) ctx.getDatabase()).getClustersIds(filterClusters);
      }
      subPlan.chain(new GetValueFromIndexEntryStep(ctx, filterClusterIds, profilingEnabled));
      if (requiresMultipleIndexLookups(desc.keyCondition) || duplicateResultsForRecord(desc)) {
        subPlan.chain(new DistinctExecutionStep(ctx, profilingEnabled));
      }
      if (desc.remainingCondition != null && !desc.remainingCondition.isEmpty()) {
        subPlan.chain(
            new FilterStep(
                createWhereFrom(desc.remainingCondition),
                ctx,
                this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                profilingEnabled));
      }
      subPlans.add(subPlan);
    }
    return new ParallelExecStep(subPlans, ctx, profilingEnabled);
  }

  /**
   * checks whether the condition has CONTAINSANY or similar expressions, that require multiple
   * index evaluations
   *
   * @param keyCondition
   * @return
   */
  private boolean requiresMultipleIndexLookups(OAndBlock keyCondition) {
    for (OBooleanExpression oBooleanExpression : keyCondition.getSubBlocks()) {
      if (!(oBooleanExpression instanceof OBinaryCondition)) {
        return true;
      }
    }
    return false;
  }

  private OWhereClause createWhereFrom(OBooleanExpression remainingCondition) {
    OWhereClause result = new OWhereClause(-1);
    result.setBaseExpression(remainingCondition);
    return result;
  }

  /**
   * given a flat AND block and a set of indexes, returns the best index to be used to process it,
   * with the complete description on how to use it
   *
   * @param ctx
   * @param indexes
   * @param block
   * @return
   */
  private IndexSearchDescriptor findBestIndexFor(
      OCommandContext ctx, Set<OIndex> indexes, OAndBlock block, OClass clazz) {
    // get all valid index descriptors
    List<IndexSearchDescriptor> descriptors =
        indexes.stream()
            .filter(x -> x.getInternal().canBeUsedInEqualityOperators())
            .map(index -> buildIndexSearchDescriptor(ctx, index, block, clazz))
            .filter(Objects::nonNull)
            .filter(x -> x.keyCondition != null)
            .filter(x -> x.keyCondition.getSubBlocks().size() > 0)
            .collect(Collectors.toList());

    List<IndexSearchDescriptor> fullTextIndexDescriptors =
        indexes.stream()
            .filter(idx -> idx.getType().equalsIgnoreCase("FULLTEXT"))
            .filter(idx -> !idx.getAlgorithm().equalsIgnoreCase("LUCENE"))
            .map(idx -> buildIndexSearchDescriptorForFulltext(ctx, idx, block, clazz))
            .filter(Objects::nonNull)
            .filter(x -> x.keyCondition != null)
            .filter(x -> x.keyCondition.getSubBlocks().size() > 0)
            .collect(Collectors.toList());

    descriptors.addAll(fullTextIndexDescriptors);

    // remove the redundant descriptors (eg. if I have one on [a] and one on [a, b], the first one
    // is redundant, just discard it)
    descriptors = removePrefixIndexes(descriptors);

    // sort by cost
    List<OPair<Integer, IndexSearchDescriptor>> sortedDescriptors =
        descriptors.stream()
            .map(x -> (OPair<Integer, IndexSearchDescriptor>) new OPair(x.cost(ctx), x))
            .sorted()
            .collect(Collectors.toList());

    // get only the descriptors with the lowest cost
    if (sortedDescriptors.isEmpty()) {
      descriptors = Collections.emptyList();
    } else {
      descriptors =
          sortedDescriptors.stream()
              .filter(x -> x.key.equals(sortedDescriptors.get(0).key))
              .map(x -> x.value)
              .collect(Collectors.toList());
    }

    // sort remaining by the number of indexed fields
    descriptors =
        descriptors.stream()
            .sorted(Comparator.comparingInt(x -> x.keyCondition.getSubBlocks().size()))
            .collect(Collectors.toList());

    // get the one that has more indexed fields
    return descriptors.isEmpty() ? null : descriptors.get(descriptors.size() - 1);
  }

  private List<IndexSearchDescriptor> removePrefixIndexes(List<IndexSearchDescriptor> descriptors) {
    List<IndexSearchDescriptor> result = new ArrayList<>();
    for (IndexSearchDescriptor desc : descriptors) {
      if (result.isEmpty()) {
        result.add(desc);
      } else {
        List<IndexSearchDescriptor> prefixes = findPrefixes(desc, result);
        if (prefixes.isEmpty()) {
          if (!isPrefixOfAny(desc, result)) {
            result.add(desc);
          }
        } else {
          result.removeAll(prefixes);
          result.add(desc);
        }
      }
    }
    return result;
  }

  private boolean isPrefixOfAny(IndexSearchDescriptor desc, List<IndexSearchDescriptor> result) {
    for (IndexSearchDescriptor item : result) {
      if (isPrefixOf(desc, item)) {
        return true;
      }
    }
    return false;
  }

  /**
   * finds prefix conditions for a given condition, eg. if the condition is on [a,b] and in the list
   * there is another condition on [a] or on [a,b], then that condition is returned.
   *
   * @param desc
   * @param descriptors
   * @return
   */
  private List<IndexSearchDescriptor> findPrefixes(
      IndexSearchDescriptor desc, List<IndexSearchDescriptor> descriptors) {
    List<IndexSearchDescriptor> result = new ArrayList<>();
    for (IndexSearchDescriptor item : descriptors) {
      if (isPrefixOf(item, desc)) {
        result.add(item);
      }
    }
    return result;
  }

  /**
   * returns true if the first argument is a prefix for the second argument, eg. if the first
   * argument is [a] and the second argument is [a, b]
   *
   * @param item
   * @param desc
   * @return
   */
  private boolean isPrefixOf(IndexSearchDescriptor item, IndexSearchDescriptor desc) {
    List<OBooleanExpression> left = item.keyCondition.getSubBlocks();
    List<OBooleanExpression> right = desc.keyCondition.getSubBlocks();
    if (left.size() > right.size()) {
      return false;
    }
    for (int i = 0; i < left.size(); i++) {
      if (!left.get(i).equals(right.get(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * given an index and a flat AND block, returns a descriptor on how to process it with an index
   * (index, index key and additional filters to apply after index fetch
   *
   * @param ctx
   * @param index
   * @param block
   * @param clazz
   * @return
   */
  private IndexSearchDescriptor buildIndexSearchDescriptor(
      OCommandContext ctx, OIndex index, OAndBlock block, OClass clazz) {
    List<String> indexFields = index.getDefinition().getFields();
    boolean found = false;

    OAndBlock blockCopy = block.copy();
    Iterator<OBooleanExpression> blockIterator;

    OAndBlock indexKeyValue = new OAndBlock(-1);
    IndexSearchDescriptor result = new IndexSearchDescriptor();
    result.idx = index;
    result.keyCondition = indexKeyValue;

    for (String indexField : indexFields) {
      OIndexSearchInfo info =
          new OIndexSearchInfo(
              indexField,
              allowsRangeQueries(index),
              isMap(clazz, indexField),
              isIndexByKey(index, indexField),
              isIndexByValue(index, indexField),
              ctx);
      blockIterator = blockCopy.getSubBlocks().iterator();
      boolean indexFieldFound = false;
      while (blockIterator.hasNext()) {
        OBooleanExpression singleExp = blockIterator.next();
        if (singleExp.isIndexAware(info)) {
          indexFieldFound = true;
          indexKeyValue.getSubBlocks().add(singleExp.copy());
          blockIterator.remove();
          if (singleExp instanceof OBinaryCondition
              && info.allowsRange()
              && ((OBinaryCondition) singleExp).getOperator().isRangeOperator()) {
            // look for the opposite condition, on the same field, for range queries (the other
            // side of the range)
            while (blockIterator.hasNext()) {
              OBooleanExpression next = blockIterator.next();
              if (next.createRangeWith(singleExp)) {
                result.additionalRangeCondition = (OBinaryCondition) next;
                blockIterator.remove();
                break;
              }
            }
          }
          break;
        }
      }

      if (indexFieldFound) {
        found = true;
      }
      if (!indexFieldFound) {
        break;
      }
    }

    if (result.keyCondition.getSubBlocks().size() < index.getDefinition().getFields().size()
        && !index.supportsOrderedIterations()) {
      // hash indexes do not support partial key match
      return null;
    }

    if (found) {
      result.remainingCondition = blockCopy;
      return result;
    }
    return null;
  }

  /**
   * given a full text index and a flat AND block, returns a descriptor on how to process it with an
   * index (index, index key and additional filters to apply after index fetch
   *
   * @param ctx
   * @param index
   * @param block
   * @param clazz
   * @return
   */
  private IndexSearchDescriptor buildIndexSearchDescriptorForFulltext(
      OCommandContext ctx, OIndex index, OAndBlock block, OClass clazz) {
    List<String> indexFields = index.getDefinition().getFields();
    boolean found = false;

    OAndBlock blockCopy = block.copy();
    Iterator<OBooleanExpression> blockIterator;

    OAndBlock indexKeyValue = new OAndBlock(-1);
    IndexSearchDescriptor result = new IndexSearchDescriptor();
    result.idx = index;
    result.keyCondition = indexKeyValue;
    for (String indexField : indexFields) {
      blockIterator = blockCopy.getSubBlocks().iterator();
      boolean indexFieldFound = false;
      while (blockIterator.hasNext()) {
        OBooleanExpression singleExp = blockIterator.next();
        if (singleExp.isFullTextIndexAware(indexField)) {
          found = true;
          indexFieldFound = true;
          indexKeyValue.getSubBlocks().add(singleExp.copy());
          blockIterator.remove();
          break;
        }
      }
      if (!indexFieldFound) {
        break;
      }
    }

    if (result.keyCondition.getSubBlocks().size() < index.getDefinition().getFields().size()
        && !index.supportsOrderedIterations()) {
      // hash indexes do not support partial key match
      return null;
    }

    if (found) {
      result.remainingCondition = blockCopy;
      return result;
    }
    return null;
  }

  private boolean isIndexByKey(OIndex index, String field) {
    OIndexDefinition def = index.getDefinition();
    for (String o : def.getFieldsToIndex()) {
      if (o.equalsIgnoreCase(field + " by key")) {
        return true;
      }
    }
    return false;
  }

  private boolean isIndexByValue(OIndex index, String field) {
    OIndexDefinition def = index.getDefinition();
    for (String o : def.getFieldsToIndex()) {
      if (o.equalsIgnoreCase(field + " by value")) {
        return true;
      }
    }
    return false;
  }

  private boolean isMap(OClass clazz, String indexField) {
    OProperty prop = clazz.getProperty(indexField);
    if (prop == null) {
      return false;
    }
    return prop.getType() == OType.EMBEDDEDMAP;
  }

  private boolean allowsRangeQueries(OIndex index) {
    return index.supportsOrderedIterations();
  }

  /**
   * aggregates multiple index conditions that refer to the same key search
   *
   * @param indexSearchDescriptors
   * @return
   */
  private List<IndexSearchDescriptor> commonFactor(
      List<IndexSearchDescriptor> indexSearchDescriptors) {
    // index, key condition, additional filter (to aggregate in OR)
    Map<OIndex, Map<IndexCondPair, OOrBlock>> aggregation = new HashMap<>();
    for (IndexSearchDescriptor item : indexSearchDescriptors) {
      Map<IndexCondPair, OOrBlock> filtersForIndex = aggregation.get(item.idx);
      if (filtersForIndex == null) {
        filtersForIndex = new HashMap<>();
        aggregation.put(item.idx, filtersForIndex);
      }
      IndexCondPair extendedCond =
          new IndexCondPair(item.keyCondition, item.additionalRangeCondition);

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
        result.add(
            new IndexSearchDescriptor(
                item.getKey(),
                filters.getKey().mainCondition,
                filters.getKey().additionalRange,
                filters.getValue()));
      }
    }
    return result;
  }

  private void handleClustersAsTarget(
      OSelectExecutionPlan plan,
      QueryPlanningInfo info,
      List<OCluster> clusters,
      OCommandContext ctx,
      boolean profilingEnabled) {
    ODatabase db = ctx.getDatabase();

    OClass candidateClass = null;
    boolean tryByIndex = true;
    Set<String> clusterNames = new HashSet<>();

    for (OCluster cluster : clusters) {
      String name = cluster.getClusterName();
      Integer clusterId = cluster.getClusterNumber();
      if (name == null) {
        name = db.getClusterNameById(clusterId);
      }
      if (clusterId == null) {
        clusterId = db.getClusterIdByName(name);
      }
      if (name != null) {
        clusterNames.add(name);
        OClass clazz =
            ((ODatabaseDocumentInternal) db)
                .getMetadata()
                .getImmutableSchemaSnapshot()
                .getClassByClusterId(clusterId);
        if (clazz == null) {
          tryByIndex = false;
          break;
        }
        if (candidateClass == null) {
          candidateClass = clazz;
        } else if (!candidateClass.equals(clazz)) {
          candidateClass = null;
          tryByIndex = false;
          break;
        }
      } else {
        tryByIndex = false;
        break;
      }
    }

    if (tryByIndex) {
      OIdentifier clazz = new OIdentifier(candidateClass.getName());
      if (handleClassAsTargetWithIndexedFunction(
          plan, clusterNames, clazz, info, ctx, profilingEnabled)) {
        return;
      }

      if (handleClassAsTargetWithIndex(plan, clazz, clusterNames, info, ctx, profilingEnabled)) {
        return;
      }

      if (info.orderBy != null
          && handleClassWithIndexForSortOnly(
              plan, clazz, clusterNames, info, ctx, profilingEnabled)) {
        return;
      }
    }

    Boolean orderByRidAsc = null; // null: no order. true: asc, false:desc
    if (isOrderByRidAsc(info)) {
      orderByRidAsc = true;
    } else if (isOrderByRidDesc(info)) {
      orderByRidAsc = false;
    }
    if (orderByRidAsc != null && info.serverToClusters.size() == 1) {
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
      FetchFromClusterExecutionStep step =
          new FetchFromClusterExecutionStep(clusterId, ctx, profilingEnabled);
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
      FetchFromClustersExecutionStep step =
          new FetchFromClustersExecutionStep(clusterIds, ctx, orderByRidAsc, profilingEnabled);
      plan.chain(step);
    }
  }

  private void handleSubqueryAsTarget(
      OSelectExecutionPlan plan,
      OStatement subQuery,
      OCommandContext ctx,
      boolean profilingEnabled) {
    OBasicCommandContext subCtx = new OBasicCommandContext();
    subCtx.setDatabase(ctx.getDatabase());
    subCtx.setParent(ctx);
    OInternalExecutionPlan subExecutionPlan =
        subQuery.createExecutionPlan(subCtx, profilingEnabled);
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
      return recordAttr != null
          && recordAttr.equalsIgnoreCase("@rid")
          && OOrderByItem.DESC.equals(item.getType());
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
      return recordAttr != null
          && recordAttr.equalsIgnoreCase("@rid")
          && (item.getType() == null || OOrderByItem.ASC.equals(item.getType()));
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
    } else return info.target.getItem().getClusterList() != null;
  }
}
