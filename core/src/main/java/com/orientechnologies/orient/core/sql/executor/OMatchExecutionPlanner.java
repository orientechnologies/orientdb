package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.sql.parser.OAndBlock;
import com.orientechnologies.orient.core.sql.parser.OCluster;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
import com.orientechnologies.orient.core.sql.parser.OFromItem;
import com.orientechnologies.orient.core.sql.parser.OGroupBy;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import com.orientechnologies.orient.core.sql.parser.OLimit;
import com.orientechnologies.orient.core.sql.parser.OMatchExpression;
import com.orientechnologies.orient.core.sql.parser.OMatchFilter;
import com.orientechnologies.orient.core.sql.parser.OMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.OMatchStatement;
import com.orientechnologies.orient.core.sql.parser.OMultiMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.ONestedProjection;
import com.orientechnologies.orient.core.sql.parser.OOrderBy;
import com.orientechnologies.orient.core.sql.parser.OProjection;
import com.orientechnologies.orient.core.sql.parser.OProjectionItem;
import com.orientechnologies.orient.core.sql.parser.ORid;
import com.orientechnologies.orient.core.sql.parser.OSelectStatement;
import com.orientechnologies.orient.core.sql.parser.OSkip;
import com.orientechnologies.orient.core.sql.parser.OUnwind;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import com.orientechnologies.orient.core.sql.parser.Pattern;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Created by luigidellaquila on 20/09/16. */
public class OMatchExecutionPlanner {

  static final String DEFAULT_ALIAS_PREFIX = "$ORIENT_DEFAULT_ALIAS_";

  protected List<OMatchExpression> matchExpressions;
  protected List<OMatchExpression> notMatchExpressions;
  protected List<OExpression> returnItems;
  protected List<OIdentifier> returnAliases;
  protected List<ONestedProjection> returnNestedProjections;
  private boolean returnElements = false;
  private boolean returnPaths = false;
  private boolean returnPatterns = false;
  private boolean returnPathElements = false;
  private boolean returnDistinct = false;
  protected OSkip skip;
  private final OGroupBy groupBy;
  private final OOrderBy orderBy;
  private final OUnwind unwind;
  protected OLimit limit;

  // post-parsing
  private Pattern pattern;
  private List<Pattern> subPatterns;
  private Map<String, OWhereClause> aliasFilters;
  private Map<String, String> aliasClasses;
  private Map<String, String> aliasClusters;
  private Map<String, ORid> aliasRids;
  private boolean foundOptional = false;
  private long threshold = 100;

  public OMatchExecutionPlanner(OMatchStatement stm) {
    this.matchExpressions =
        stm.getMatchExpressions().stream().map(x -> x.copy()).collect(Collectors.toList());
    this.notMatchExpressions =
        stm.getNotMatchExpressions().stream().map(x -> x.copy()).collect(Collectors.toList());
    this.returnItems =
        stm.getReturnItems().stream().map(x -> x.copy()).collect(Collectors.toList());
    this.returnAliases =
        stm.getReturnAliases().stream()
            .map(x -> x == null ? null : x.copy())
            .collect(Collectors.toList());
    this.returnNestedProjections =
        stm.getReturnNestedProjections().stream()
            .map(x -> x == null ? null : x.copy())
            .collect(Collectors.toList());
    this.limit = stm.getLimit() == null ? null : stm.getLimit().copy();
    this.skip = stm.getSkip() == null ? null : stm.getSkip().copy();

    this.returnElements = stm.returnsElements();
    this.returnPaths = stm.returnsPaths();
    this.returnPatterns = stm.returnsPatterns();
    this.returnPathElements = stm.returnsPathElements();
    this.returnDistinct = stm.isReturnDistinct();
    this.groupBy = stm.getGroupBy() == null ? null : stm.getGroupBy().copy();
    this.orderBy = stm.getOrderBy() == null ? null : stm.getOrderBy().copy();
    this.unwind = stm.getUnwind() == null ? null : stm.getUnwind().copy();
  }

  public OInternalExecutionPlan createExecutionPlan(
      OCommandContext context, boolean enableProfiling) {

    buildPatterns(context);
    splitDisjointPatterns(context);

    OSelectExecutionPlan result = new OSelectExecutionPlan(context);
    Map<String, Long> estimatedRootEntries =
        estimateRootEntries(aliasClasses, aliasClusters, aliasRids, aliasFilters, context);
    Set<String> aliasesToPrefetch =
        estimatedRootEntries.entrySet().stream()
            .filter(x -> x.getValue() < this.threshold)
            .filter(x -> !dependsOnExecutionContext(x.getKey()))
            .map(x -> x.getKey())
            .collect(Collectors.toSet());
    for (Map.Entry<String, Long> entry : estimatedRootEntries.entrySet()) {
      if (entry.getValue() == 0L && !isOptional(entry.getKey())) {
        result.chain(new EmptyStep(context, enableProfiling));
        return result;
      }
    }

    addPrefetchSteps(result, aliasesToPrefetch, context, enableProfiling);

    if (subPatterns.size() > 1) {
      CartesianProductStep step = new CartesianProductStep(context, enableProfiling);
      for (Pattern subPattern : subPatterns) {
        step.addSubPlan(
            createPlanForPattern(
                subPattern, context, estimatedRootEntries, aliasesToPrefetch, enableProfiling));
      }
      result.chain(step);
    } else {
      OInternalExecutionPlan plan =
          createPlanForPattern(
              pattern, context, estimatedRootEntries, aliasesToPrefetch, enableProfiling);
      for (OExecutionStep step : plan.getSteps()) {
        result.chain((OExecutionStepInternal) step);
      }
    }

    manageNotPatterns(result, pattern, notMatchExpressions, context, enableProfiling);

    if (foundOptional) {
      result.chain(new RemoveEmptyOptionalsStep(context, enableProfiling));
    }

    if (returnElements || returnPaths || returnPatterns || returnPathElements) {
      addReturnStep(result, context, enableProfiling);

      if (this.returnDistinct) {
        result.chain(new DistinctExecutionStep(context, enableProfiling));
      }
      if (groupBy != null) {
        throw new OCommandExecutionException(
            "Cannot execute GROUP BY in MATCH query with RETURN $elements, $pathElements, $patterns or $paths");
      }

      if (this.orderBy != null) {
        result.chain(new OrderByStep(orderBy, context, -1, enableProfiling));
      }

      if (this.unwind != null) {
        result.chain(new UnwindStep(unwind, context, enableProfiling));
      }

      if (this.skip != null && skip.getValue(context) >= 0) {
        result.chain(new SkipExecutionStep(skip, context, enableProfiling));
      }
      if (this.limit != null && limit.getValue(context) >= 0) {
        result.chain(new LimitExecutionStep(limit, context, enableProfiling));
      }
    } else {
      QueryPlanningInfo info = new QueryPlanningInfo();
      List<OProjectionItem> items = new ArrayList<>();
      for (int i = 0; i < this.returnItems.size(); i++) {
        OProjectionItem item =
            new OProjectionItem(
                returnItems.get(i), this.returnAliases.get(i), returnNestedProjections.get(i));
        items.add(item);
      }
      info.projection = new OProjection(items, returnDistinct);

      info.projection = OSelectExecutionPlanner.translateDistinct(info.projection);
      info.distinct = info.projection == null ? false : info.projection.isDistinct();
      if (info.projection != null) {
        info.projection.setDistinct(false);
      }

      info.groupBy = this.groupBy;
      info.orderBy = this.orderBy;
      info.unwind = this.unwind;
      info.skip = this.skip;
      info.limit = this.limit;

      OSelectExecutionPlanner.optimizeQuery(info, context);
      OSelectExecutionPlanner.handleProjectionsBlock(result, info, context, enableProfiling);
    }

    return result;
  }

  private boolean dependsOnExecutionContext(String key) {
    OWhereClause filter = aliasFilters.get(key);
    if (filter == null) {
      return false;
    }
    if (filter.refersToParent()) {
      return true;
    }
    if (filter.toString().toLowerCase().contains("$matched.")) {
      return true;
    }
    return false;
  }

  private boolean isOptional(String key) {
    PatternNode node = this.pattern.aliasToNode.get(key);
    return node != null && node.isOptionalNode();
  }

  private void manageNotPatterns(
      OSelectExecutionPlan result,
      Pattern pattern,
      List<OMatchExpression> notMatchExpressions,
      OCommandContext context,
      boolean enableProfiling) {
    for (OMatchExpression exp : notMatchExpressions) {
      if (pattern.aliasToNode.get(exp.getOrigin().getAlias()) == null) {
        throw new OCommandExecutionException(
            "This kind of NOT expression is not supported (yet). "
                + "The first alias in a NOT expression has to be present in the positive pattern");
      }

      if (exp.getOrigin().getFilter() != null) {
        throw new OCommandExecutionException(
            "This kind of NOT expression is not supported (yet): "
                + "WHERE condition on the initial alias");
        // TODO implement his
      }

      OMatchFilter lastFilter = exp.getOrigin();
      List<AbstractExecutionStep> steps = new ArrayList<>();
      for (OMatchPathItem item : exp.getItems()) {
        if (item instanceof OMultiMatchPathItem) {
          throw new OCommandExecutionException(
              "This kind of NOT expression is not supported (yet): " + item.toString());
        }
        PatternEdge edge = new PatternEdge();
        edge.item = item;
        edge.out = new PatternNode();
        edge.out.alias = lastFilter.getAlias();
        edge.in = new PatternNode();
        edge.in.alias = item.getFilter().getAlias();
        EdgeTraversal traversal = new EdgeTraversal(edge, true);
        MatchStep step = new MatchStep(context, traversal, enableProfiling);
        steps.add(step);
        lastFilter = item.getFilter();
      }
      result.chain(new FilterNotMatchPatternStep(steps, context, enableProfiling));
    }
  }

  private void addReturnStep(
      OSelectExecutionPlan result, OCommandContext context, boolean profilingEnabled) {
    if (returnElements) {
      result.chain(new ReturnMatchElementsStep(context, profilingEnabled));
    } else if (returnPaths) {
      result.chain(new ReturnMatchPathsStep(context, profilingEnabled));
    } else if (returnPatterns) {
      result.chain(new ReturnMatchPatternsStep(context, profilingEnabled));
    } else if (returnPathElements) {
      result.chain(new ReturnMatchPathElementsStep(context, profilingEnabled));
    } else {
      OProjection projection = new OProjection(-1);
      projection.setItems(new ArrayList<>());
      for (int i = 0; i < returnAliases.size(); i++) {
        OProjectionItem item = new OProjectionItem(-1);
        item.setExpression(returnItems.get(i));
        item.setAlias(returnAliases.get(i));
        item.setNestedProjection(returnNestedProjections.get(i));
        projection.getItems().add(item);
      }
      result.chain(new ProjectionCalculationStep(projection, context, profilingEnabled));
    }
  }

  private OInternalExecutionPlan createPlanForPattern(
      Pattern pattern,
      OCommandContext context,
      Map<String, Long> estimatedRootEntries,
      Set<String> prefetchedAliases,
      boolean profilingEnabled) {
    OSelectExecutionPlan plan = new OSelectExecutionPlan(context);
    List<EdgeTraversal> sortedEdges = getTopologicalSortedSchedule(estimatedRootEntries, pattern);

    boolean first = true;
    if (sortedEdges.size() > 0) {
      for (EdgeTraversal edge : sortedEdges) {
        if (edge.edge.out.alias != null) {
          edge.setLeftClass(aliasClasses.get(edge.edge.out.alias));
          edge.setLeftCluster(aliasClusters.get(edge.edge.out.alias));
          edge.setLeftRid(aliasRids.get(edge.edge.out.alias));
          edge.setLeftClass(aliasClasses.get(edge.edge.out.alias));
          edge.setLeftFilter(aliasFilters.get(edge.edge.out.alias));
        }
        addStepsFor(plan, edge, context, first, profilingEnabled);
        first = false;
      }
    } else {
      PatternNode node = pattern.getAliasToNode().values().iterator().next();
      if (prefetchedAliases.contains(node.alias)) {
        // from prefetch
        plan.chain(new MatchFirstStep(context, node, profilingEnabled));
      } else {
        // from actual execution plan
        String clazz = aliasClasses.get(node.alias);
        String cluster = aliasClusters.get(node.alias);
        ORid rid = aliasRids.get(node.alias);
        OWhereClause filter = aliasFilters.get(node.alias);
        OSelectStatement select = createSelectStatement(clazz, cluster, rid, filter);
        plan.chain(
            new MatchFirstStep(
                context,
                node,
                select.createExecutionPlan(context, profilingEnabled),
                profilingEnabled));
      }
    }
    return plan;
  }

  /** sort edges in the order they will be matched */
  private List<EdgeTraversal> getTopologicalSortedSchedule(
      Map<String, Long> estimatedRootEntries, Pattern pattern) {
    List<EdgeTraversal> resultingSchedule = new ArrayList<>();
    Map<String, Set<String>> remainingDependencies = getDependencies(pattern);
    Set<PatternNode> visitedNodes = new HashSet<>();
    Set<PatternEdge> visitedEdges = new HashSet<>();

    // Sort the possible root vertices in order of estimated size, since we want to start with a
    // small vertex set.
    List<OPair<Long, String>> rootWeights = new ArrayList<>();
    for (Map.Entry<String, Long> root : estimatedRootEntries.entrySet()) {
      rootWeights.add(new OPair<>(root.getValue(), root.getKey()));
    }
    Collections.sort(rootWeights);

    // Add the starting vertices, in the correct order, to an ordered set.
    Set<String> remainingStarts = new LinkedHashSet<String>();
    for (OPair<Long, String> item : rootWeights) {
      remainingStarts.add(item.getValue());
    }
    // Add all the remaining aliases after all the suggested start points.
    for (String alias : pattern.aliasToNode.keySet()) {
      if (!remainingStarts.contains(alias)) {
        remainingStarts.add(alias);
      }
    }

    while (resultingSchedule.size() < pattern.numOfEdges) {
      // Start a new depth-first pass, adding all nodes with satisfied dependencies.
      // 1. Find a starting vertex for the depth-first pass.
      PatternNode startingNode = null;
      List<String> startsToRemove = new ArrayList<String>();
      for (String currentAlias : remainingStarts) {
        PatternNode currentNode = pattern.aliasToNode.get(currentAlias);

        if (visitedNodes.contains(currentNode)) {
          // If a previous traversal already visited this alias, remove it from further
          // consideration.
          startsToRemove.add(currentAlias);
        } else if (remainingDependencies.get(currentAlias) == null
            || remainingDependencies.get(currentAlias).isEmpty()) {
          // If it hasn't been visited, and has all dependencies satisfied, visit it.
          startsToRemove.add(currentAlias);
          startingNode = currentNode;
          break;
        }
      }
      remainingStarts.removeAll(startsToRemove);

      if (startingNode == null) {
        // We didn't manage to find a valid root, and yet we haven't constructed a complete
        // schedule.
        // This means there must be a cycle in our dependency graph, or all dependency-free nodes
        // are optional.
        // Therefore, the query is invalid.
        throw new OCommandExecutionException(
            "This query contains MATCH conditions that cannot be evaluated, "
                + "like an undefined alias or a circular dependency on a $matched condition.");
      }

      // 2. Having found a starting vertex, traverse its neighbors depth-first,
      //    adding any non-visited ones with satisfied dependencies to our schedule.
      updateScheduleStartingAt(
          startingNode, visitedNodes, visitedEdges, remainingDependencies, resultingSchedule);
    }

    if (resultingSchedule.size() != pattern.numOfEdges) {
      throw new AssertionError(
          "Incorrect number of edges: " + resultingSchedule.size() + " vs " + pattern.numOfEdges);
    }

    return resultingSchedule;
  }

  /**
   * Start a depth-first traversal from the starting node, adding all viable unscheduled edges and
   * vertices.
   *
   * @param startNode the node from which to start the depth-first traversal
   * @param visitedNodes set of nodes that are already visited (mutated in this function)
   * @param visitedEdges set of edges that are already visited and therefore don't need to be
   *     scheduled (mutated in this function)
   * @param remainingDependencies dependency map including only the dependencies that haven't yet
   *     been satisfied (mutated in this function)
   * @param resultingSchedule the schedule being computed i.e. appended to (mutated in this
   *     function)
   */
  private void updateScheduleStartingAt(
      PatternNode startNode,
      Set<PatternNode> visitedNodes,
      Set<PatternEdge> visitedEdges,
      Map<String, Set<String>> remainingDependencies,
      List<EdgeTraversal> resultingSchedule) {
    // OrientDB requires the schedule to contain all edges present in the query, which is a stronger
    // condition
    // than simply visiting all nodes in the query. Consider the following example query:
    //     MATCH {
    //         class: A,
    //         as: foo
    //     }.in() {
    //         as: bar
    //     }, {
    //         class: B,
    //         as: bar
    //     }.out() {
    //         as: foo
    //     } RETURN $matches
    // The schedule for the above query must have two edges, even though there are only two nodes
    // and they can both
    // be visited with the traversal of a single edge.
    //
    // To satisfy it, we obey the following for each non-optional node:
    // - ignore edges to neighboring nodes which have unsatisfied dependencies;
    // - for visited neighboring nodes, add their edge if it wasn't already present in the schedule,
    // but do not
    //   recurse into the neighboring node;
    // - for unvisited neighboring nodes with satisfied dependencies, add their edge and recurse
    // into them.
    visitedNodes.add(startNode);
    for (Set<String> dependencies : remainingDependencies.values()) {
      dependencies.remove(startNode.alias);
    }

    Map<PatternEdge, Boolean> edges = new LinkedHashMap<PatternEdge, Boolean>();
    for (PatternEdge outEdge : startNode.out) {
      edges.put(outEdge, true);
    }
    for (PatternEdge inEdge : startNode.in) {
      if (inEdge.item.isBidirectional()) {
        edges.put(inEdge, false);
      }
    }

    for (Map.Entry<PatternEdge, Boolean> edgeData : edges.entrySet()) {
      PatternEdge edge = edgeData.getKey();
      boolean isOutbound = edgeData.getValue();
      PatternNode neighboringNode = isOutbound ? edge.in : edge.out;

      if (!remainingDependencies.get(neighboringNode.alias).isEmpty()) {
        // Unsatisfied dependencies, ignore this neighboring node.
        continue;
      }

      if (visitedNodes.contains(neighboringNode)) {
        if (!visitedEdges.contains(edge)) {
          // If we are executing in this block, we are in the following situation:
          // - the startNode has not been visited yet;
          // - it has a neighboringNode that has already been visited;
          // - the edge between the startNode and the neighboringNode has not been scheduled yet.
          //
          // The isOutbound value shows us whether the edge is outbound from the point of view of
          // the startNode.
          // However, if there are edges to the startNode, we must visit the startNode from an
          // already-visited
          // neighbor, to preserve the validity of the traversal. Therefore, we negate the value of
          // isOutbound
          // to ensure that the edge is always scheduled in the direction from the already-visited
          // neighbor
          // toward the startNode. Notably, this is also the case when evaluating "optional" nodes
          // -- we always
          // visit the optional node from its non-optional and already-visited neighbor.
          //
          // The only exception to the above is when we have edges with "while" conditions. We are
          // not allowed
          // to flip their directionality, so we leave them as-is.
          boolean traversalDirection;
          if (startNode.optional || edge.item.isBidirectional()) {
            traversalDirection = !isOutbound;
          } else {
            traversalDirection = isOutbound;
          }

          visitedEdges.add(edge);
          resultingSchedule.add(new EdgeTraversal(edge, traversalDirection));
        }
      } else if (!startNode.optional || isOptionalChain(startNode, edge, neighboringNode)) {
        // If the neighboring node wasn't visited, we don't expand the optional node into it, hence
        // the above check.
        // Instead, we'll allow the neighboring node to add the edge we failed to visit, via the
        // above block.
        if (visitedEdges.contains(edge)) {
          // Should never happen.
          throw new AssertionError(
              "The edge was visited, but the neighboring vertex was not: "
                  + edge
                  + " "
                  + neighboringNode);
        }

        visitedEdges.add(edge);
        resultingSchedule.add(new EdgeTraversal(edge, isOutbound));
        updateScheduleStartingAt(
            neighboringNode, visitedNodes, visitedEdges, remainingDependencies, resultingSchedule);
      }
    }
  }

  private boolean isOptionalChain(
      PatternNode startNode, PatternEdge edge, PatternNode neighboringNode) {
    return isOptionalChain(startNode, edge, neighboringNode, new HashSet<>());
  }

  private boolean isOptionalChain(
      PatternNode startNode,
      PatternEdge edge,
      PatternNode neighboringNode,
      Set<PatternEdge> visitedEdges) {
    if (!startNode.isOptionalNode() || !neighboringNode.isOptionalNode()) {
      return false;
    }

    visitedEdges.add(edge);

    if (neighboringNode.out != null) {
      for (PatternEdge patternEdge : neighboringNode.out) {
        if (!visitedEdges.contains(patternEdge)
            && !isOptionalChain(neighboringNode, patternEdge, patternEdge.in, visitedEdges)) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Calculate the set of dependency aliases for each alias in the pattern.
   *
   * @param pattern
   * @return map of alias to the set of aliases it depends on
   */
  private Map<String, Set<String>> getDependencies(Pattern pattern) {
    Map<String, Set<String>> result = new HashMap<String, Set<String>>();

    for (PatternNode node : pattern.aliasToNode.values()) {
      Set<String> currentDependencies = new HashSet<String>();

      OWhereClause filter = aliasFilters.get(node.alias);
      if (filter != null && filter.getBaseExpression() != null) {
        List<String> involvedAliases = filter.getBaseExpression().getMatchPatternInvolvedAliases();
        if (involvedAliases != null) {
          currentDependencies.addAll(involvedAliases);
        }
      }

      result.put(node.alias, currentDependencies);
    }

    return result;
  }

  private void splitDisjointPatterns(OCommandContext context) {
    if (this.subPatterns != null) {
      return;
    }

    this.subPatterns = pattern.getDisjointPatterns();
  }

  private void addStepsFor(
      OSelectExecutionPlan plan,
      EdgeTraversal edge,
      OCommandContext context,
      boolean first,
      boolean profilingEnabled) {
    if (first) {
      PatternNode patternNode = edge.out ? edge.edge.out : edge.edge.in;
      String clazz = this.aliasClasses.get(patternNode.alias);
      String cluster = this.aliasClusters.get(patternNode.alias);
      ORid rid = this.aliasRids.get(patternNode.alias);
      OWhereClause where = aliasFilters.get(patternNode.alias);
      OSelectStatement select = new OSelectStatement(-1);
      select.setTarget(new OFromClause(-1));
      select.getTarget().setItem(new OFromItem(-1));
      if (clazz != null) {
        select.getTarget().getItem().setIdentifier(new OIdentifier(clazz));
      } else if (cluster != null) {
        select.getTarget().getItem().setCluster(new OCluster(cluster));
      } else if (rid != null) {
        select.getTarget().getItem().setRids(Collections.singletonList(rid));
      }
      select.setWhereClause(where == null ? null : where.copy());
      OBasicCommandContext subContxt = new OBasicCommandContext();
      subContxt.setParentWithoutOverridingChild(context);
      plan.chain(
          new MatchFirstStep(
              context,
              patternNode,
              select.createExecutionPlan(subContxt, profilingEnabled),
              profilingEnabled));
    }
    if (edge.edge.in.isOptionalNode()) {
      foundOptional = true;
      plan.chain(new OptionalMatchStep(context, edge, profilingEnabled));
    } else {
      plan.chain(new MatchStep(context, edge, profilingEnabled));
    }
  }

  private void addPrefetchSteps(
      OSelectExecutionPlan result,
      Set<String> aliasesToPrefetch,
      OCommandContext context,
      boolean profilingEnabled) {
    for (String alias : aliasesToPrefetch) {
      String targetClass = aliasClasses.get(alias);
      String targetCluster = aliasClusters.get(alias);
      ORid targetRid = aliasRids.get(alias);
      OWhereClause filter = aliasFilters.get(alias);
      OSelectStatement prefetchStm =
          createSelectStatement(targetClass, targetCluster, targetRid, filter);

      MatchPrefetchStep step =
          new MatchPrefetchStep(
              context,
              prefetchStm.createExecutionPlan(context, profilingEnabled),
              alias,
              profilingEnabled);
      result.chain(step);
    }
  }

  private OSelectStatement createSelectStatement(
      String targetClass, String targetCluster, ORid targetRid, OWhereClause filter) {
    OSelectStatement prefetchStm = new OSelectStatement(-1);
    prefetchStm.setWhereClause(filter);
    OFromClause from = new OFromClause(-1);
    OFromItem fromItem = new OFromItem(-1);
    if (targetRid != null) {
      fromItem.setRids(Collections.singletonList(targetRid));
    } else if (targetClass != null) {
      fromItem.setIdentifier(new OIdentifier(targetClass));
    } else if (targetCluster != null) {
      fromItem.setCluster(new OCluster(targetCluster));
    }
    from.setItem(fromItem);
    prefetchStm.setTarget(from);
    return prefetchStm;
  }

  private void buildPatterns(OCommandContext ctx) {
    if (this.pattern != null) {
      return;
    }
    List<OMatchExpression> allPatterns = new ArrayList<>();
    allPatterns.addAll(this.matchExpressions);
    allPatterns.addAll(this.notMatchExpressions);

    assignDefaultAliases(allPatterns);

    pattern = new Pattern();
    for (OMatchExpression expr : this.matchExpressions) {
      pattern.addExpression(expr);
    }

    Map<String, OWhereClause> aliasFilters = new LinkedHashMap<>();
    Map<String, String> aliasClasses = new LinkedHashMap<>();
    Map<String, String> aliasClusters = new LinkedHashMap<>();
    Map<String, ORid> aliasRids = new LinkedHashMap<>();
    for (OMatchExpression expr : this.matchExpressions) {
      addAliases(expr, aliasFilters, aliasClasses, aliasClusters, aliasRids, ctx);
    }

    this.aliasFilters = aliasFilters;
    this.aliasClasses = aliasClasses;
    this.aliasClusters = aliasClusters;
    this.aliasRids = aliasRids;

    rebindFilters(aliasFilters);
  }

  private void rebindFilters(Map<String, OWhereClause> aliasFilters) {
    for (OMatchExpression expression : matchExpressions) {
      OWhereClause newFilter = aliasFilters.get(expression.getOrigin().getAlias());
      expression.getOrigin().setFilter(newFilter);

      for (OMatchPathItem item : expression.getItems()) {
        newFilter = aliasFilters.get(item.getFilter().getAlias());
        item.getFilter().setFilter(newFilter);
      }
    }
  }

  private void addAliases(
      OMatchExpression expr,
      Map<String, OWhereClause> aliasFilters,
      Map<String, String> aliasClasses,
      Map<String, String> aliasClusters,
      Map<String, ORid> aliasRids,
      OCommandContext context) {
    addAliases(expr.getOrigin(), aliasFilters, aliasClasses, aliasClusters, aliasRids, context);
    for (OMatchPathItem item : expr.getItems()) {
      if (item.getFilter() != null) {
        addAliases(item.getFilter(), aliasFilters, aliasClasses, aliasClusters, aliasRids, context);
      }
    }
  }

  private void addAliases(
      OMatchFilter matchFilter,
      Map<String, OWhereClause> aliasFilters,
      Map<String, String> aliasClasses,
      Map<String, String> aliasClusters,
      Map<String, ORid> aliasRids,
      OCommandContext context) {
    String alias = matchFilter.getAlias();
    OWhereClause filter = matchFilter.getFilter();
    if (alias != null) {
      if (filter != null && filter.getBaseExpression() != null) {
        OWhereClause previousFilter = aliasFilters.get(alias);
        if (previousFilter == null) {
          previousFilter = new OWhereClause(-1);
          previousFilter.setBaseExpression(new OAndBlock(-1));
          aliasFilters.put(alias, previousFilter);
        }
        OAndBlock filterBlock = (OAndBlock) previousFilter.getBaseExpression();
        if (filter != null && filter.getBaseExpression() != null) {
          filterBlock.getSubBlocks().add(filter.getBaseExpression());
        }
      }

      String clazz = matchFilter.getClassName(context);
      if (clazz != null) {
        String previousClass = aliasClasses.get(alias);
        if (previousClass == null) {
          aliasClasses.put(alias, clazz);
        } else {
          String lower = getLowerSubclass(context.getDatabase(), clazz, previousClass);
          if (lower == null) {
            throw new OCommandExecutionException(
                "classes defined for alias "
                    + alias
                    + " ("
                    + clazz
                    + ", "
                    + previousClass
                    + ") are not in the same hierarchy");
          }
          aliasClasses.put(alias, lower);
        }
      }

      String clusterName = matchFilter.getClusterName(context);
      if (clusterName != null) {
        String previousCluster = aliasClusters.get(alias);
        if (previousCluster == null) {
          aliasClusters.put(alias, clusterName);
        } else if (!previousCluster.equalsIgnoreCase(clusterName)) {
          throw new OCommandExecutionException(
              "Invalid expression for alias "
                  + alias
                  + " cannot be of both clusters "
                  + previousCluster
                  + " and "
                  + clusterName);
        }
      }

      ORid rid = matchFilter.getRid(context);
      if (rid != null) {
        ORid previousRid = aliasRids.get(alias);
        if (previousRid == null) {
          aliasRids.put(alias, rid);
        } else if (!previousRid.equals(rid)) {
          throw new OCommandExecutionException(
              "Invalid expression for alias "
                  + alias
                  + " cannot be of both RIDs "
                  + previousRid
                  + " and "
                  + rid);
        }
      }
    }
  }

  private String getLowerSubclass(ODatabase db, String className1, String className2) {
    OSchema schema = db.getMetadata().getSchema();
    OClass class1 = schema.getClass(className1);
    OClass class2 = schema.getClass(className2);
    if (class1.isSubClassOf(class2)) {
      return class1.getName();
    }
    if (class2.isSubClassOf(class1)) {
      return class2.getName();
    }
    return null;
  }

  /**
   * assigns default aliases to pattern nodes that do not have an explicit alias
   *
   * @param matchExpressions
   */
  private void assignDefaultAliases(List<OMatchExpression> matchExpressions) {
    int counter = 0;
    for (OMatchExpression expression : matchExpressions) {
      if (expression.getOrigin().getAlias() == null) {
        expression.getOrigin().setAlias(DEFAULT_ALIAS_PREFIX + (counter++));
      }

      for (OMatchPathItem item : expression.getItems()) {
        if (item.getFilter() == null) {
          item.setFilter(new OMatchFilter(-1));
        }
        if (item.getFilter().getAlias() == null) {
          item.getFilter().setAlias(DEFAULT_ALIAS_PREFIX + (counter++));
        }
      }
    }
  }

  private Map<String, Long> estimateRootEntries(
      Map<String, String> aliasClasses,
      Map<String, String> aliasClusters,
      Map<String, ORid> aliasRids,
      Map<String, OWhereClause> aliasFilters,
      OCommandContext ctx) {
    Set<String> allAliases = new LinkedHashSet<String>();
    allAliases.addAll(aliasClasses.keySet());
    allAliases.addAll(aliasFilters.keySet());
    allAliases.addAll(aliasClusters.keySet());
    allAliases.addAll(aliasRids.keySet());

    OSchema schema =
        ((ODatabaseDocumentInternal) ctx.getDatabase()).getMetadata().getImmutableSchemaSnapshot();

    Map<String, Long> result = new LinkedHashMap<String, Long>();
    for (String alias : allAliases) {
      ORid rid = aliasRids.get(alias);
      if (rid != null) {
        result.put(alias, 1L);
        continue;
      }

      String className = aliasClasses.get(alias);
      String clusterName = aliasClusters.get(alias);

      if (className == null && clusterName == null) {
        continue;
      }

      if (className != null) {
        if (!schema.existsClass(className)) {
          throw new OCommandExecutionException("class not defined: " + className);
        }
        OClass oClass = schema.getClass(className);
        long upperBound;
        OWhereClause filter = aliasFilters.get(alias);
        if (filter != null) {
          upperBound = filter.estimate(oClass, this.threshold, ctx);
        } else {
          upperBound = oClass.count();
        }
        result.put(alias, upperBound);
      } else if (clusterName != null) {
        ODatabase db = ctx.getDatabase();
        if (!db.existsCluster(clusterName)) {
          throw new OCommandExecutionException("cluster not defined: " + clusterName);
        }
        int clusterId = db.getClusterIdByName(clusterName);
        OClass oClass = db.getMetadata().getSchema().getClassByClusterId(clusterId);
        if (oClass != null) {
          long upperBound;
          OWhereClause filter = aliasFilters.get(alias);
          if (filter != null) {
            upperBound =
                Math.min(
                    db.countClusterElements(clusterName),
                    filter.estimate(oClass, this.threshold, ctx));
          } else {
            upperBound = db.countClusterElements(clusterName);
          }
          result.put(alias, upperBound);
        } else {
          result.put(alias, db.countClusterElements(clusterName));
        }
      }
    }
    return result;
  }
}
