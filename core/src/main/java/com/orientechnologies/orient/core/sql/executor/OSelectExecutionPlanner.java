package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexAbstract;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import com.orientechnologies.orient.core.sql.executor.metadata.OClassIndexFinder;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexCandidate;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexCandidateOne;
import com.orientechnologies.orient.core.sql.executor.metadata.OSpecificIndexFinder;
import com.orientechnologies.orient.core.sql.parser.AggregateProjectionSplit;
import com.orientechnologies.orient.core.sql.parser.OAndBlock;
import com.orientechnologies.orient.core.sql.parser.OBaseExpression;
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OBinaryCondition;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import com.orientechnologies.orient.core.sql.parser.OCluster;
import com.orientechnologies.orient.core.sql.parser.OEqualsCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OExecutionPlanCache;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
import com.orientechnologies.orient.core.sql.parser.OFromItem;
import com.orientechnologies.orient.core.sql.parser.OFunctionCall;
import com.orientechnologies.orient.core.sql.parser.OGeOperator;
import com.orientechnologies.orient.core.sql.parser.OGroupBy;
import com.orientechnologies.orient.core.sql.parser.OGtOperator;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import com.orientechnologies.orient.core.sql.parser.OIndexIdentifier;
import com.orientechnologies.orient.core.sql.parser.OInputParameter;
import com.orientechnologies.orient.core.sql.parser.OInteger;
import com.orientechnologies.orient.core.sql.parser.OLeOperator;
import com.orientechnologies.orient.core.sql.parser.OLetClause;
import com.orientechnologies.orient.core.sql.parser.OLetItem;
import com.orientechnologies.orient.core.sql.parser.OLtOperator;
import com.orientechnologies.orient.core.sql.parser.OMetadataIdentifier;
import com.orientechnologies.orient.core.sql.parser.OOrderBy;
import com.orientechnologies.orient.core.sql.parser.OOrderByItem;
import com.orientechnologies.orient.core.sql.parser.OProjection;
import com.orientechnologies.orient.core.sql.parser.OProjectionItem;
import com.orientechnologies.orient.core.sql.parser.ORecordAttribute;
import com.orientechnologies.orient.core.sql.parser.ORid;
import com.orientechnologies.orient.core.sql.parser.OSelectStatement;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.sql.parser.OTimeout;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import com.orientechnologies.orient.core.sql.parser.SubQueryCollector;
import com.orientechnologies.orient.core.transaction.ONodeId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
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

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx, boolean useCache) {
    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) ctx.getDatabase();

    long planningStart = System.currentTimeMillis();

    init(ctx);
    OSelectExecutionPlan result = new OSelectExecutionPlan();

    if (info.expand && info.distinct) {
      throw new OCommandExecutionException(
          "Cannot execute a statement with DISTINCT expand(), please use a subquery");
    }

    optimizeQuery(info, ctx);

    if (handleHardwiredOptimizations(result, ctx)) {
      return result;
    }

    handleGlobalLet(result, info, ctx);

    calculateShardingStrategy(info, ctx);

    handleFetchFromTarger(result, info, ctx);

    if (info.globalLetPresent) {
      // do the raw fetch remotely, then do the rest on the coordinator
      buildDistributedExecutionPlan(result, info, ctx);
    }

    handleLet(result, info);

    handleLockRecord(result, info);
    handleWhere(result, info);

    // TODO optimization: in most cases the projections can be calculated on remote nodes
    buildDistributedExecutionPlan(result, info, ctx);

    handleProjectionsBlock(result, info, ctx);

    if (info.timeout != null) {
      result.chain(new AccumulatingTimeoutStep(info.timeout));
    }

    if (useCache
        && !ctx.isProfiling()
        && statement.executinPlanCanBeCached()
        && result.canBeCached()
        && OExecutionPlanCache.getLastInvalidation(db) < planningStart) {
      OExecutionPlanCache.put(
          statement.getOriginalStatement(), result, (ODatabaseDocumentInternal) ctx.getDatabase());
    }
    return result;
  }

  private void handleLockRecord(OSelectExecutionPlan result, QueryPlanningInfo info) {
    if (info.lockRecord != null) {
      if (info.distributedPlanCreated) {
        result.chain(new LockRecordStep(info.lockRecord));
      } else {
        for (OSelectExecutionPlan shardedPlan : info.distributedFetchExecutionPlans.values()) {
          shardedPlan.chain(new LockRecordStep(info.lockRecord));
        }
      }
    }
  }

  public static void handleProjectionsBlock(
      OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx) {
    handleProjectionsBeforeOrderBy(result, info, ctx);

    if (info.expand || info.unwind != null || info.groupBy != null) {

      handleProjections(result, info, ctx);
      handleExpand(result, info);
      handleUnwind(result, info);
      handleOrderBy(result, info, ctx);
      if (info.skip != null) {
        result.chain(new SkipExecutionStep(info.skip));
      }
      if (info.limit != null) {
        result.chain(new LimitExecutionStep(info.limit));
      }
    } else {
      handleOrderBy(result, info, ctx);
      if (info.distinct || info.groupBy != null || info.aggregateProjection != null) {
        handleProjections(result, info, ctx);
        handleDistinct(result, info, ctx);
        if (info.skip != null) {
          result.chain(new SkipExecutionStep(info.skip));
        }
        if (info.limit != null) {
          result.chain(new LimitExecutionStep(info.limit));
        }
      } else {
        if (info.skip != null) {
          result.chain(new SkipExecutionStep(info.skip));
        }
        if (info.limit != null) {
          result.chain(new LimitExecutionStep(info.limit));
        }
        handleProjections(result, info, ctx);
      }
    }
  }

  private void buildDistributedExecutionPlan(
      OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx) {
    if (info.distributedFetchExecutionPlans == null) {
      return;
    }
    ONodeId currentNode =
        ((ODatabaseDocumentInternal) ctx.getDatabase())
            .getSharedContext()
            .getOrientDB()
            .getNodeId();
    if (info.distributedFetchExecutionPlans.size() == 1) {
      if (info.distributedFetchExecutionPlans.get(currentNode) != null) {
        // everything is executed on local server
        OSelectExecutionPlan localSteps = info.distributedFetchExecutionPlans.get(currentNode);
        for (OExecutionStepInternal step : localSteps.getSteps()) {
          result.chain(step);
        }
      } else {
        // everything is executed on a single remote node
        ONodeId node = info.distributedFetchExecutionPlans.keySet().iterator().next();
        OSelectExecutionPlan subPlan = info.distributedFetchExecutionPlans.get(node);
        DistributedExecutionStep step = new DistributedExecutionStep(subPlan, node);
        result.chain(step);
      }
      info.distributedFetchExecutionPlans = null;
    } else {
      // sharded fetching
      List<OInternalExecutionPlan> subPlans = new ArrayList<>();
      for (Map.Entry<ONodeId, OSelectExecutionPlan> entry :
          info.distributedFetchExecutionPlans.entrySet()) {
        if (entry.getKey().equals(currentNode)) {
          subPlans.add(entry.getValue());
        } else {
          DistributedExecutionStep step =
              new DistributedExecutionStep(entry.getValue(), entry.getKey());
          OSelectExecutionPlan subPlan = new OSelectExecutionPlan();
          subPlan.chain(step);
          subPlans.add(subPlan);
        }
      }
      result.chain(new ParallelExecStep(subPlans));
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
    ONodeId localNodeId = db.getSharedContext().getOrientDB().getNodeId();

    Collection<String> readClusterNames = db.getClusterNames();
    Set<String> clusterNames;
    if (readClusterNames instanceof Set) {
      clusterNames = (Set<String>) readClusterNames;
    } else {
      clusterNames = new HashSet<>(readClusterNames);
    }
    if (!db.isSharded()) {
      info.serverToClusters = new LinkedHashMap<>();
      info.serverToClusters.put(localNodeId, clusterNames);
      info.distributedFetchExecutionPlans.put(localNodeId, new OSelectExecutionPlan());
      return;
    }

    //    Map<String, Set<String>> clusterMap = db.getActiveClusterMap();
    Map<ONodeId, Set<String>> clusterMap = new HashMap<>();
    clusterMap.put(localNodeId, new HashSet<>(clusterNames));

    Set<String> queryClusters = calculateTargetClusters(info, ctx);
    if (queryClusters == null || queryClusters.size() == 0) { // no target

      info.serverToClusters = new LinkedHashMap<>();
      info.serverToClusters.put(localNodeId, clusterMap.get(localNodeId));
      info.distributedFetchExecutionPlans.put(localNodeId, new OSelectExecutionPlan());
      return;
    }

    //    Set<String> serversWithAllTheClusers = getServersThatHasAllClusters(clusterMap,
    // queryClusters);
    //    if (serversWithAllTheClusers.isEmpty()) {
    // sharded query
    Map<ONodeId, Set<String>> minimalSetOfNodes =
        getMinimalSetOfNodesForShardedQuery(
            db.getSharedContext().getOrientDB().getNodeId(), clusterMap, queryClusters);
    if (minimalSetOfNodes == null) {
      throw new OCommandExecutionException("Cannot execute sharded query");
    }
    info.serverToClusters = minimalSetOfNodes;
    for (ONodeId node : info.serverToClusters.keySet()) {
      info.distributedFetchExecutionPlans.put(node, new OSelectExecutionPlan());
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
  private Map<ONodeId, Set<String>> getMinimalSetOfNodesForShardedQuery(
      ONodeId localNode, Map<ONodeId, Set<String>> clusterMap, Set<String> queryClusters) {
    // approximate algorithm, the problem is NP-complete
    Map<ONodeId, Set<String>> result = new LinkedHashMap<>();
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
      ONodeId nextNode = findItemThatCoversMore(uncovered, clusterMap);
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

  private ONodeId findItemThatCoversMore(
      Set<String> uncovered, Map<ONodeId, Set<String>> clusterMap) {
    ONodeId lastFound = null;
    int lastSize = -1;
    for (Map.Entry<ONodeId, Set<String>> nodeConfig : clusterMap.entrySet()) {
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
  private Set<ONodeId> getServersThatHasAllClusters(
      Map<ONodeId, Set<String>> clusterMap, Set<String> queryClusters) {
    Set<ONodeId> remainingServers = clusterMap.keySet();
    for (String cluster : queryClusters) {
      for (Map.Entry<ONodeId, Set<String>> serverConfig : clusterMap.entrySet()) {
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
      return Collections.emptySet();
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

  private boolean handleHardwiredOptimizations(OSelectExecutionPlan result, OCommandContext ctx) {
    if (handleHardwiredCountOnIndex(result, info)) {
      return true;
    }
    if (handleHardwiredCountOnClass(result, info, ctx)) {
      return true;
    }
    return handleHardwiredCountOnClassUsingIndex(result, info, ctx);
  }

  private boolean handleHardwiredCountOnClass(
      OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx) {
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
        new CountFromClassStep(targetClass, info.projection.getAllAliases().iterator().next()));
    return true;
  }

  private boolean securityPoliciesExistForClass(OIdentifier targetClass, OCommandContext ctx) {
    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) ctx.getDatabase();
    OSecurityInternal security = db.getSharedContext().getSecurity();
    OImmutableClass clazz =
        (OImmutableClass)
            db.getMetadata()
                .getImmutableSchemaSnapshot()
                .getClass(targetClass.getStringValue()); // normalize class name case
    if (clazz == null) {
      return false;
    }
    if (clazz.isRestricted()) {
      return true;
    }
    return security.isReadRestrictedBySecurityPolicy(db, "database.class." + clazz.getName());
  }

  private boolean handleHardwiredCountOnClassUsingIndex(
      OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx) {
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
    if (info.whereClause == null
        || info.whereClause.isEmpty()
        || info.whereClause.conditionsCount() > 1) {
      // for now it only handles a single equality condition, it can be extended
      return false;
    }
    OBooleanExpression condition = info.whereClause.getBaseExpression();
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
                info.projection.getAllAliases().iterator().next()));
        return true;
      }
    }

    return false;
  }

  private boolean handleHardwiredCountOnIndex(OSelectExecutionPlan result, QueryPlanningInfo info) {
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
        new CountFromIndexStep(targetIndex, info.projection.getAllAliases().iterator().next()));
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

  public static void handleUnwind(OSelectExecutionPlan result, QueryPlanningInfo info) {
    if (info.unwind != null) {
      result.chain(new UnwindStep(info.unwind));
    }
  }

  private static void handleDistinct(
      OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx) {
    if (info.distinct) {
      result.chain(new DistinctExecutionStep(ctx));
    }
  }

  private static void handleProjectionsBeforeOrderBy(
      OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx) {
    if (info.orderBy != null) {
      handleProjections(result, info, ctx);
    }
  }

  private static void handleProjections(
      OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx) {
    if (!info.projectionsCalculated && info.projection != null) {
      if (info.preAggregateProjection != null) {
        result.chain(new ProjectionCalculationStep(info.preAggregateProjection));
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
                info.timeout != null ? info.timeout.getVal().longValue() : -1));
        if (isCountOnly(info) && info.groupBy == null) {
          result.chain(new GuaranteeEmptyCountStep(info.aggregateProjection.getItems().get(0)));
        }
      }
      result.chain(new ProjectionCalculationStep(info.projection));

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
            && item.getExpression().getMathExpression() != null
            && !item.getExpression().getMathExpression().isParentesis()
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
    if (orderBy != null && orderBy.getItems() != null && !orderBy.getItems().isEmpty()) {
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
    } else {
      handleGroupByNoAggragation(info);
    }
  }

  private static void handleGroupByNoAggragation(QueryPlanningInfo info) {
    if (info.groupBy == null
        || info.groupBy.getItems() == null
        || info.groupBy.getItems().size() == 0) {
      return;
    }
    for (OExpression exp : info.groupBy.getItems()) {
      if (exp.isAggregate()) {
        throw new OCommandExecutionException("Cannot group by an aggregate function");
      }
      OProjectionItem newItem = new OProjectionItem(-1);
      newItem.setExpression(exp);
      if (info.aggregateProjection == null) {
        info.aggregateProjection = new OProjection(-1);
      }
      if (info.aggregateProjection.getItems() == null) {
        info.aggregateProjection.setItems(new ArrayList<>());
      }
      info.aggregateProjection.getItems().add(newItem);
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
      OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx) {

    OFromItem target = info.target == null ? null : info.target.getItem();
    for (Map.Entry<ONodeId, OSelectExecutionPlan> shardedPlan :
        info.distributedFetchExecutionPlans.entrySet()) {
      if (target == null) {
        handleNoTarget(shardedPlan.getValue());
      } else if (target.getIdentifier() != null) {
        String className = target.getIdentifier().getStringValue();
        if (className.startsWith("$")
            && !((ODatabaseDocumentInternal) ctx.getDatabase())
                .getMetadata()
                .getImmutableSchemaSnapshot()
                .existsClass(className)) {
          handleVariableAsTarget(shardedPlan.getValue(), info);
        } else {
          Set<String> filterClusters = info.serverToClusters.get(shardedPlan.getKey());
          if (info.whereClause != null) {
            OAndBlock ridRangeConditions = info.whereClause.extractRidRanges(ctx);
            if (ridRangeConditions != null && !ridRangeConditions.isEmpty()) {
              info.ridRangeConditions = ridRangeConditions;
              filterClusters =
                  filterClusters.stream()
                      .filter(
                          x ->
                              clusterMatchesRidRange(x, ridRangeConditions, ctx.getDatabase(), ctx))
                      .collect(Collectors.toSet());
            }
          }

          handleClassAsTarget(shardedPlan.getValue(), filterClusters, info, ctx);
        }
      } else if (target.getCluster() != null) {
        handleClustersAsTarget(
            shardedPlan.getValue(), info, Collections.singletonList(target.getCluster()), ctx);
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
        handleClustersAsTarget(shardedPlan.getValue(), info, clustersForShard, ctx);
      } else if (target.getStatement() != null) {
        handleSubqueryAsTarget(shardedPlan.getValue(), target.getStatement(), ctx);
      } else if (target.getFunctionCall() != null) {
        //        handleFunctionCallAsTarget(result, target.getFunctionCall(), ctx);//TODO
        throw new OCommandExecutionException("function call as target is not supported yet");
      } else if (target.getInputParam() != null) {
        handleInputParamAsTarget(
            shardedPlan.getValue(),
            info.serverToClusters.get(shardedPlan.getKey()),
            info,
            target.getInputParam(),
            ctx);
      } else if (target.getInputParams() != null && target.getInputParams().size() > 0) {
        List<OInternalExecutionPlan> plans = new ArrayList<>();
        for (OInputParameter param : target.getInputParams()) {
          OSelectExecutionPlan subPlan = new OSelectExecutionPlan();
          handleInputParamAsTarget(
              subPlan, info.serverToClusters.get(shardedPlan.getKey()), info, param, ctx);
          plans.add(subPlan);
        }
        shardedPlan.getValue().chain(new ParallelExecStep(plans));
      } else if (target.getIndex() != null) {
        handleIndexAsTarget(shardedPlan.getValue(), info, target.getIndex(), null, ctx);
        if (info.serverToClusters.size() > 1) {
          shardedPlan
              .getValue()
              .chain(new FilterByClustersStep(info.serverToClusters.get(shardedPlan.getKey())));
        }
      } else if (target.getMetadata() != null) {
        handleMetadataAsTarget(shardedPlan.getValue(), target.getMetadata(), ctx);
      } else if (target.getRids() != null && target.getRids().size() > 0) {
        Set<String> filterClusters = info.serverToClusters.get(shardedPlan.getKey());
        List<ORid> rids = new ArrayList<>();
        for (ORid rid : target.getRids()) {
          if (filterClusters == null || isFromClusters(rid, filterClusters, ctx.getDatabase())) {
            rids.add(rid);
          }
        }
        if (rids.size() > 0) {
          handleRidsAsTarget(shardedPlan.getValue(), rids, ctx);
        } else {
          result.chain(new EmptyStep()); // nothing to return
        }
      } else if (target.isEmptyList()) {
        result.chain(new EmptyStep());
      } else {
        throw new UnsupportedOperationException();
      }
    }
  }

  private void handleVariableAsTarget(OSelectExecutionPlan plan, QueryPlanningInfo info) {
    plan.chain(new FetchFromVariableStep(info.target.getItem()));
  }

  private boolean clusterMatchesRidRange(
      String clusterName,
      OAndBlock ridRangeConditions,
      ODatabaseSession database,
      OCommandContext ctx) {
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

  private void handleInputParamAsTarget(
      OSelectExecutionPlan result,
      Set<String> filterClusters,
      QueryPlanningInfo info,
      OInputParameter inputParam,
      OCommandContext ctx) {
    Object paramValue = inputParam.getValue(ctx.getInputParameters());
    if (paramValue == null) {
      result.chain(new EmptyStep()); // nothing to return
    } else if (paramValue instanceof OClass) {
      OFromClause from = new OFromClause(-1);
      OFromItem item = new OFromItem(-1);
      from.setItem(item);
      item.setIdentifier(new OIdentifier(((OClass) paramValue).getName()));
      handleClassAsTarget(result, filterClusters, from, info, ctx);
    } else if (paramValue instanceof String) {
      // strings are treated as classes
      OFromClause from = new OFromClause(-1);
      OFromItem item = new OFromItem(-1);
      from.setItem(item);
      item.setIdentifier(new OIdentifier((String) paramValue));
      handleClassAsTarget(result, filterClusters, from, info, ctx);
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
        handleRidsAsTarget(result, Collections.singletonList(rid), ctx);
      } else {
        result.chain(new EmptyStep()); // nothing to return
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
        handleRidsAsTarget(result, rids, ctx);
      } else {
        result.chain(new EmptyStep()); // nothing to return
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
  private boolean isFromClusters(ORid rid, Set<String> filterClusters, ODatabaseSession database) {
    if (filterClusters == null) {
      throw new IllegalArgumentException();
    }
    String clusterName = database.getClusterNameById(rid.getCluster().getValue().intValue());
    return filterClusters.contains(clusterName);
  }

  private void handleNoTarget(OSelectExecutionPlan result) {
    result.chain(new EmptyDataGeneratorStep(1));
  }

  private void handleIndexAsTarget(
      OSelectExecutionPlan result,
      QueryPlanningInfo info,
      OIndexIdentifier indexIdentifier,
      Set<String> filterClusters,
      OCommandContext ctx) {

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
        OBooleanExpression ridCondition = null;
        Optional<OIndexCandidate> found;
        if (info.whereClause == null || info.whereClause.isEmpty()) {
          if (!index.supportsOrderedIterations()) {
            throw new OCommandExecutionException(
                "Index " + indexName + " does not allow iteration without a condition");
          }
          found = Optional.empty();
        } else {
          ridCondition = info.whereClause.getIndexRidCondition();
          found = info.whereClause.findIndex(new OSpecificIndexFinder(index), ctx);
          found = found.flatMap((x) -> x.normalize(ctx)).flatMap((x) -> x.finalize(ctx));
          if (found.isEmpty()) {
            throw new OCommandExecutionException(
                "Index queries with this kind of condition are not supported yet: "
                    + info.whereClause);
          }
        }

        OIndexCandidate candidate = found.orElseGet(() -> new OIndexCandidateOne(index.getName()));
        result.chain(new FetchFromIndexStep(candidate, true, ctx));
        if (ridCondition != null) {
          OWhereClause where = new OWhereClause(-1);
          where.setBaseExpression(ridCondition);
          result.chain(
              new FilterStep(
                  where,
                  this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                  this.info.isExclusiveLock()));
        }
        break;
      case VALUES:
      case VALUESASC:
        if (!index.supportsOrderedIterations()) {
          throw new OCommandExecutionException(
              "Index " + indexName + " does not allow iteration on values");
        }
        result.chain(
            new FetchFromIndexValuesStep(new OIndexCandidateOne(index.getName()), true, ctx));
        result.chain(new GetValueFromIndexEntryStep(filterClusterIds));
        break;
      case VALUESDESC:
        if (!index.supportsOrderedIterations()) {
          throw new OCommandExecutionException(
              "Index " + indexName + " does not allow iteration on values");
        }
        result.chain(
            new FetchFromIndexValuesStep(new OIndexCandidateOne(index.getName()), false, ctx));
        result.chain(new GetValueFromIndexEntryStep(filterClusterIds));
        break;
    }
  }

  private void handleMetadataAsTarget(
      OSelectExecutionPlan plan, OMetadataIdentifier metadata, OCommandContext ctx) {
    ODatabaseInternal db = (ODatabaseInternal) ctx.getDatabase();
    String schemaRecordIdAsString = null;
    if (metadata.getName().equalsIgnoreCase("schema")) {
      schemaRecordIdAsString = db.getStorageInfo().getConfiguration().getSchemaRecordId();
      ORecordId schemaRid = new ORecordId(schemaRecordIdAsString);
      plan.chain(new FetchFromRidsStep(Collections.singleton(schemaRid)));
    } else if (metadata.getName().equalsIgnoreCase("indexmanager")) {
      schemaRecordIdAsString = db.getStorageInfo().getConfiguration().getIndexMgrRecordId();
      ORecordId schemaRid = new ORecordId(schemaRecordIdAsString);
      plan.chain(new FetchFromRidsStep(Collections.singleton(schemaRid)));
    } else if (metadata.getName().equalsIgnoreCase("storage")) {
      plan.chain(new FetchFromStorageMetadataStep());
    } else if (metadata.getName().equalsIgnoreCase("database")) {
      plan.chain(new FetchFromDatabaseMetadataStep());
    } else if (metadata.getName().equalsIgnoreCase("distributed")) {
      plan.chain(new FetchFromDistributedMetadataStep());
    } else {
      throw new UnsupportedOperationException("Invalid metadata: " + metadata.getName());
    }
  }

  private void handleRidsAsTarget(OSelectExecutionPlan plan, List<ORid> rids, OCommandContext ctx) {
    List<ORecordId> actualRids = new ArrayList<>();
    for (ORid rid : rids) {
      actualRids.add(rid.toRecordId((OResult) null, ctx));
    }
    plan.chain(new FetchFromRidsStep(actualRids));
  }

  private static void handleExpand(OSelectExecutionPlan result, QueryPlanningInfo info) {
    if (info.expand) {
      result.chain(new ExpandStep());
    }
  }

  private void handleGlobalLet(
      OSelectExecutionPlan result, QueryPlanningInfo info, OCommandContext ctx) {
    if (info.globalLetClause != null) {
      List<OLetItem> items = info.globalLetClause.getItems();
      items = sortLet(items, this.statement.getLetClause());
      List<String> scriptVars = new ArrayList<>();
      for (OLetItem item : items) {
        ctx.declareScriptVariable(item.getVarName().getStringValue());
        if (item.getExpression() != null) {
          result.chain(new GlobalLetExpressionStep(item.getVarName(), item.getExpression()));
        } else {
          result.chain(new GlobalLetQueryStep(item.getVarName(), item.getQuery(), ctx, scriptVars));
        }
        scriptVars.add(item.getVarName().getStringValue());
        info.globalLetPresent = true;
      }
    }
  }

  private void handleLet(OSelectExecutionPlan plan, QueryPlanningInfo info) {
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
            plan.chain(new LetExpressionStep(item.getVarName(), item.getExpression()));
          } else {
            plan.chain(new LetQueryStep(item.getVarName(), item.getQuery()));
          }
        }
      } else {
        for (OSelectExecutionPlan shardedPlan : info.distributedFetchExecutionPlans.values()) {
          for (OLetItem item : items) {
            if (item.getExpression() != null) {
              shardedPlan.chain(
                  new LetExpressionStep(item.getVarName().copy(), item.getExpression().copy()));
            } else {
              shardedPlan.chain(new LetQueryStep(item.getVarName().copy(), item.getQuery().copy()));
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

  private void handleWhere(OSelectExecutionPlan plan, QueryPlanningInfo info) {
    if (info.whereClause != null) {
      if (info.distributedPlanCreated) {
        plan.chain(
            new FilterStep(
                info.whereClause,
                this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                this.info.isExclusiveLock()));
      } else {
        for (OSelectExecutionPlan shardedPlan : info.distributedFetchExecutionPlans.values()) {
          shardedPlan.chain(
              new FilterStep(
                  info.whereClause.copy(),
                  this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                  this.info.isExclusiveLock()));
        }
      }
    }
  }

  public static void handleOrderBy(
      OSelectExecutionPlan plan, QueryPlanningInfo info, OCommandContext ctx) {
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
              info.timeout != null ? info.timeout.getVal().longValue() : -1));
      if (info.projectionAfterOrderBy != null) {
        plan.chain(new ProjectionCalculationStep(info.projectionAfterOrderBy));
      }
    }
  }

  /**
   * @param plan the execution plan where to add the fetch step
   * @param filterClusters clusters of interest (all the others have to be excluded from the result)
   * @param info
   * @param ctx
   */
  private void handleClassAsTarget(
      OSelectExecutionPlan plan,
      Set<String> filterClusters,
      QueryPlanningInfo info,
      OCommandContext ctx) {
    handleClassAsTarget(plan, filterClusters, info.target, info, ctx);
  }

  private void handleClassAsTarget(
      OSelectExecutionPlan plan,
      Set<String> filterClusters,
      OFromClause from,
      QueryPlanningInfo info,
      OCommandContext ctx) {
    OIdentifier identifier = from.getItem().getIdentifier();
    if (handleClassAsTargetWithIndexedFunction(plan, filterClusters, identifier, info, ctx)) {
      plan.chain(new FilterByClassStep(identifier));
      return;
    }

    if (handleClassAsTargetWithIndex(plan, identifier, filterClusters, info, ctx)) {
      plan.chain(new FilterByClassStep(identifier));
      return;
    }

    if (info.orderBy != null
        && handleClassWithIndexForSortOnly(plan, identifier, filterClusters, info, ctx)) {
      plan.chain(new FilterByClassStep(identifier));
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
          new FetchFromClassExecutionStep(className, filterClusters, info, ctx, orderByRidAsc);
    } else if (schema.getView(className) != null) {
      fetcher = new FetchFromViewExecutionStep(className, filterClusters, info, ctx, orderByRidAsc);
    } else {
      throw new OCommandExecutionException("Class or View not present in the schema: " + className);
    }

    if (orderByRidAsc != null && info.serverToClusters.size() == 1) {
      info.orderApplied = true;
    }
    plan.chain(fetcher);
  }

  private int[] classClustersFiltered(
      ODatabaseSession db, OClass clazz, Set<String> filterClusters) {
    int[] ids = clazz.getPolymorphicClusterIds();
    List<Integer> filtered = new ArrayList<>();
    for (int id : ids) {
      if (filterClusters.contains(db.getClusterNameById(id))) {
        filtered.add(id);
      }
    }
    int[] result = new int[filtered.size()];
    for (int i = 0; i < filtered.size(); i++) {
      result[i] = filtered.get(i);
    }
    return result;
  }

  private boolean handleClassAsTargetWithIndexedFunction(
      OSelectExecutionPlan plan,
      Set<String> filterClusters,
      OIdentifier queryTarget,
      QueryPlanningInfo info,
      OCommandContext ctx) {
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
    if (info.whereClause == null) {
      return false;
    }

    List<OInternalExecutionPlan> resultSubPlans = new ArrayList<>();

    boolean indexedFunctionsFound = false;

    List<OBinaryCondition> indexedFunctionConditions =
        info.whereClause.getIndexedFunctionConditions(
            clazz, (ODatabaseDocumentInternal) ctx.getDatabase());

    indexedFunctionConditions =
        filterIndexedFunctionsWithoutIndex(indexedFunctionConditions, info.target, ctx);

    if (indexedFunctionConditions == null || indexedFunctionConditions.size() == 0) {
      List<OExecutionStepInternal> result =
          handleClassAsTargetWithIndex(clazz.getName(), filterClusters, info, ctx);
      if (result != null) {
        OSelectExecutionPlan subPlan = new OSelectExecutionPlan();
        for (OExecutionStepInternal step : result) {
          subPlan.chain(step);
        }
        resultSubPlans.add(subPlan);
      } else {
        FetchFromClassExecutionStep step;
        if (clazz instanceof OView) {
          step = new FetchFromViewExecutionStep(clazz.getName(), filterClusters, info, ctx, true);
        } else {
          step = new FetchFromClassExecutionStep(clazz.getName(), filterClusters, ctx, true);
        }
        OSelectExecutionPlan subPlan = new OSelectExecutionPlan();
        subPlan.chain(step);
        if ((info.perRecordLetClause != null /*&& refersToLet(block.getSubBlocks())*/)) {
          handleLet(subPlan, info);
        }
        subPlan.chain(
            new FilterStep(
                info.whereClause,
                this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
                this.info.isExclusiveLock()));
        resultSubPlans.add(subPlan);
      }
    } else {
      OBinaryCondition blockCandidateFunction = null;
      for (OBinaryCondition cond : indexedFunctionConditions) {
        if (!cond.allowsIndexedFunctionExecutionOnTarget(info.target, ctx)) {
          if (!cond.canExecuteIndexedFunctionWithoutIndex(info.target, ctx)) {
            throw new OCommandExecutionException(
                "Cannot execute " + info.whereClause + " on " + queryTarget);
          }
        }
        if (blockCandidateFunction == null) {
          blockCandidateFunction = cond;
        } else {
          boolean thisAllowsNoIndex = cond.canExecuteIndexedFunctionWithoutIndex(info.target, ctx);
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
          new FetchFromIndexedFunctionStep(blockCandidateFunction, info.target);
      plan.chain(step);
      plan.chain(new FilterByClustersStep(filterClusters));
      if ((info.perRecordLetClause != null /*&& refersToLet(block.getSubBlocks())*/)) {
        handleLet(plan, info);
      }
      plan.chain(
          new FilterStep(
              this.info.whereClause,
              this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
              this.info.isExclusiveLock()));

      indexedFunctionsFound = true;
    }

    if (indexedFunctionsFound) {
      if (resultSubPlans.size()
          > 0) { // if resultSubPlans.size() == 1 the step was already chained (see above)
        plan.chain(new ParallelExecStep(resultSubPlans));
        plan.chain(new FilterByClustersStep(filterClusters));
        plan.chain(new DistinctExecutionStep(ctx));
      }
      // WHERE condition already applied
      info.whereClause = null;
      return true;
    } else {
      return false;
    }
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
      OCommandContext ctx) {
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
        if (orderItem.getModifier() != null) {
          return false;
        }

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
                new OIndexCandidateOne(idx.getName()), orderType.equals(OOrderByItem.ASC), ctx));
        int[] filterClusterIds = null;
        if (filterClusters != null) {
          filterClusterIds = classClustersFiltered(ctx.getDatabase(), clazz, filterClusters);
        } else {
          filterClusterIds = clazz.getPolymorphicClusterIds();
        }
        plan.chain(new GetValueFromIndexEntryStep(filterClusterIds));
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
      OCommandContext ctx) {

    List<OExecutionStepInternal> result =
        handleClassAsTargetWithIndex(targetClass.getStringValue(), filterClusters, info, ctx);
    if (result != null) {
      result.stream().forEach(x -> plan.chain(x));
      info.whereClause = null;
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
          handleClassAsTargetWithIndexRecursive(subClass.getName(), filterClusters, info, ctx);
      if (subSteps == null || subSteps.size() == 0) {
        return false;
      }
      OSelectExecutionPlan subPlan = new OSelectExecutionPlan();
      subSteps.stream().forEach(x -> subPlan.chain(x));
      subclassPlans.add(subPlan);
    }
    if (subclassPlans.size() > 0) {
      plan.chain(new ParallelExecStep(subclassPlans));
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
      String targetClass, Set<String> filterClusters, QueryPlanningInfo info, OCommandContext ctx) {
    List<OExecutionStepInternal> result =
        handleClassAsTargetWithIndex(targetClass, filterClusters, info, ctx);
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
            handleClassAsTargetWithIndexRecursive(subClass.getName(), filterClusters, info, ctx);
        if (subSteps == null || subSteps.size() == 0) {
          return null;
        }
        OSelectExecutionPlan subPlan = new OSelectExecutionPlan();
        subSteps.stream().forEach(x -> subPlan.chain(x));
        subclassPlans.add(subPlan);
      }
      if (subclassPlans.size() > 0) {
        result.add(new ParallelExecStep(subclassPlans));
      }
    }
    return result.size() == 0 ? null : result;
  }

  private List<OExecutionStepInternal> handleClassAsTargetWithIndex(
      String targetClass, Set<String> filterClusters, QueryPlanningInfo info, OCommandContext ctx) {
    if (info.whereClause == null || info.whereClause.isEmpty()) {
      return null;
    }
    OSchema schema = getSchemaFromContext(ctx);
    OClass clazz = schema.getClass(targetClass);
    if (clazz == null) {
      clazz = schema.getView(targetClass);
    }
    if (clazz == null) {
      throw new OCommandExecutionException("Cannot find class " + targetClass);
    }

    OClassIndexFinder finder = new OClassIndexFinder(targetClass);
    Optional<OIndexCandidate> found = info.whereClause.findIndex(finder, ctx);
    if (found.isEmpty()) {
      return null; // Nothing found
    }
    found = found.get().normalize(ctx);
    if (found.isEmpty()) {
      return null; // some blocks could not be managed with an index
    }
    found = found.get().finalize(ctx);
    if (found.isEmpty()) {
      return null; // some blocks could not be managed with an index
    }
    OIndexCandidate candidate = found.get();

    List<OExecutionStepInternal> result = null;
    result = executionStepFromIndexes(filterClusters, clazz, info, ctx, candidate);
    return result;
  }

  private List<OExecutionStepInternal> executionStepFromIndexes(
      Set<String> filterClusters,
      OClass clazz,
      QueryPlanningInfo info,
      OCommandContext ctx,
      OIndexCandidate candidate) {
    List<OExecutionStepInternal> result;
    result = new ArrayList<>();
    Boolean orderAsc = getOrderDirection(info);
    result.add(new FetchFromIndexStep(candidate, !Boolean.FALSE.equals(orderAsc), ctx));
    int[] filterClusterIds = null;
    if (filterClusters != null) {
      filterClusterIds = classClustersFiltered(ctx.getDatabase(), clazz, filterClusters);
    } else {
      filterClusterIds = clazz.getPolymorphicClusterIds();
    }
    result.add(new GetValueFromIndexEntryStep(filterClusterIds));
    if (candidate.requiresDistinctStep(ctx)) {
      result.add(new DistinctExecutionStep(ctx));
    }
    if (orderAsc != null
        && info.orderBy != null
        && fullySorted(info.orderBy, candidate, ctx)
        && info.serverToClusters.size() == 1) {
      info.orderApplied = true;
    }

    result.add(
        new FilterStep(
            info.whereClause,
            this.info.timeout != null ? this.info.timeout.getVal().longValue() : -1,
            this.info.isExclusiveLock()));
    return result;
  }

  private static OSchema getSchemaFromContext(OCommandContext ctx) {
    return ((OMetadataInternal) ctx.getDatabase().getMetadata()).getImmutableSchemaSnapshot();
  }

  private boolean fullySorted(OOrderBy orderBy, OIndexCandidate desc, OCommandContext ctx) {
    if (orderBy.ordersWithCollate() || !orderBy.ordersSameDirection() || orderBy.ordersNested()) {
      return false;
    }
    return desc.fullySorted(orderBy.getProperties(), ctx);
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

  private void handleClustersAsTarget(
      OSelectExecutionPlan plan,
      QueryPlanningInfo info,
      List<OCluster> clusters,
      OCommandContext ctx) {
    ODatabaseSession db = ctx.getDatabase();

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

    if (tryByIndex && candidateClass != null) {
      OIdentifier clazz = new OIdentifier(candidateClass.getName());
      if (handleClassAsTargetWithIndexedFunction(plan, clusterNames, clazz, info, ctx)) {
        return;
      }

      if (handleClassAsTargetWithIndex(plan, clazz, clusterNames, info, ctx)) {
        return;
      }

      if (info.orderBy != null
          && handleClassWithIndexForSortOnly(plan, clazz, clusterNames, info, ctx)) {
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
