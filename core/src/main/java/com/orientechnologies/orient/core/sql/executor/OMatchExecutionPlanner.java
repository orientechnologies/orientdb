package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 20/09/16.
 */
public class OMatchExecutionPlanner {

  static final String DEFAULT_ALIAS_PREFIX = "$ORIENT_DEFAULT_ALIAS_";

  protected List<OMatchExpression> matchExpressions;
  protected List<OExpression>      returnItems;
  protected List<OIdentifier>      returnAliases;
  boolean returnElements     = false;
  boolean returnPaths        = false;
  boolean returnPatterns     = false;
  boolean returnPathElements = false;
  protected OSkip  skip;
  protected OLimit limit;

  //post-parsing
  private Pattern                   pattern;
  private List<Pattern>             subPatterns;
  private Map<String, OWhereClause> aliasFilters;
  private Map<String, String>       aliasClasses;
  boolean foundOptional = false;
  private long threshold = 100;

  public OMatchExecutionPlanner(OMatchStatement stm) {
    this.matchExpressions = stm.getMatchExpressions().stream().map(x -> x.copy()).collect(Collectors.toList());
    this.returnItems = stm.getReturnItems().stream().map(x -> x.copy()).collect(Collectors.toList());
    this.returnAliases = stm.getReturnAliases().stream().map(x -> x == null ? null : x.copy()).collect(Collectors.toList());
    this.limit = stm.getLimit() == null ? null : stm.getLimit().copy();
    //    this.skip = stm.getSkip() == null ? null : stm.getSkip().copy();

    this.returnElements = stm.returnsElements();
    this.returnPaths = stm.returnsPaths();
    this.returnPatterns = stm.returnsPatterns();
    this.returnPathElements = stm.returnsPathElements();
  }

  public OInternalExecutionPlan createExecutionPlan(OCommandContext context) {

    buildPatterns(context);
    splitDisjointPatterns(context);

    OSelectExecutionPlan result = new OSelectExecutionPlan(context);
    Map<String, Long> estimatedRootEntries = estimateRootEntries(aliasClasses, aliasFilters, context);
    Set<String> aliasesToPrefetch = estimatedRootEntries.entrySet().stream().filter(x -> x.getValue() < this.threshold).
        map(x -> x.getKey()).collect(Collectors.toSet());
    if (estimatedRootEntries.values().contains(0l)) {
      return result;
    }

    addPrefetchSteps(result, aliasesToPrefetch, context);

    if (subPatterns.size() > 1) {
      CartesianProductStep step = new CartesianProductStep(context);
      for (Pattern subPattern : subPatterns) {
        step.addSubPlan(createPlanForPattern(subPattern, context, estimatedRootEntries, aliasesToPrefetch));
      }
      result.chain(step);
    } else {
      OInternalExecutionPlan plan = createPlanForPattern(pattern, context, estimatedRootEntries, aliasesToPrefetch);
      for (OExecutionStep step : plan.getSteps()) {
        result.chain((OExecutionStepInternal) step);
      }
    }

    if(foundOptional){
      result.chain(new RemoveEmptyOptionalsStep(context));
    }
    addReturnStep(result, context);

    result.chain(new DistinctExecutionStep(context)); //TODO make it optional?

    if (this.skip != null && skip.getValue(context) >= 0) {
      result.chain(new SkipExecutionStep(skip, context));
    }
    if (this.limit != null && limit.getValue(context) >= 0) {
      result.chain(new LimitExecutionStep(limit, context));
    }

    return result;

  }

  private void addReturnStep(OSelectExecutionPlan result, OCommandContext context) {
    if (returnElements) {
      result.chain(new ReturnMatchElementsStep(context));
    } else if (returnPaths) {
      result.chain(new ReturnMatchPathsStep(context));
    } else if (returnPatterns) {
      result.chain(new ReturnMatchPatternsStep(context));
    } else if (returnPathElements) {
      result.chain(new ReturnMatchPathElementsStep(context));
    } else {
      OProjection projection = new OProjection(-1);
      projection.setItems(new ArrayList<>());
      for (int i = 0; i < returnAliases.size(); i++) {
        OProjectionItem item = new OProjectionItem(-1);
        item.setExpression(returnItems.get(i));
        item.setAlias(returnAliases.get(i));
        projection.getItems().add(item);
      }
      result.chain(new ProjectionCalculationStep(projection, context));
    }
  }

  private OInternalExecutionPlan createPlanForPattern(Pattern pattern, OCommandContext context,
      Map<String, Long> estimatedRootEntries, Set<String> prefetchedAliases) {
    OSelectExecutionPlan plan = new OSelectExecutionPlan(context);
    List<EdgeTraversal> sortedEdges = sortEdges(estimatedRootEntries, pattern, context);

    boolean first = true;
    if (sortedEdges.size() > 0) {
      for (EdgeTraversal edge : sortedEdges) {
        addStepsFor(plan, edge, context, first);
        first = false;
      }
    } else {
      PatternNode node = pattern.getAliasToNode().values().iterator().next();
      if (prefetchedAliases.contains(node.alias)) {
        //from prefetch
        plan.chain(new MatchFirstStep(context, node));
      } else {
        //from actual execution plan
        String clazz = aliasClasses.get(node.alias);
        OWhereClause filter = aliasFilters.get(node.alias);
        OSelectStatement select = createSelectStatement(clazz, filter);
        plan.chain(new MatchFirstStep(context, node, select.createExecutionPlan(context)));
      }
    }
    return plan;
  }

  private void splitDisjointPatterns(OCommandContext context) {
    if (this.subPatterns != null) {
      return;
    }

    this.subPatterns = pattern.getDisjointPatterns();
  }

  private void addStepsFor(OSelectExecutionPlan plan, EdgeTraversal edge, OCommandContext context, boolean first) {
    if (first) {
      plan.chain(new MatchFirstStep(context, edge.out ? edge.edge.out : edge.edge.in));
    }
    if(edge.edge.in.isOptionalNode()){
      foundOptional = true;
      plan.chain(new OptionalMatchStep(context, edge));
    }else {
      plan.chain(new MatchStep(context, edge));
    }
  }

  private void addPrefetchSteps(OSelectExecutionPlan result, Set<String> aliasesToPrefetch, OCommandContext context) {
    for (String alias : aliasesToPrefetch) {
      String targetClass = aliasClasses.get(alias);
      OWhereClause filter = aliasFilters.get(alias);
      OSelectStatement prefetchStm = createSelectStatement(targetClass, filter);

      MatchPrefetchStep step = new MatchPrefetchStep(context, prefetchStm.createExecutionPlan(context), alias);
      result.chain(step);
    }
  }

  private OSelectStatement createSelectStatement(String targetClass, OWhereClause filter) {
    OSelectStatement prefetchStm = new OSelectStatement(-1);
    prefetchStm.setWhereClause(filter);
    OFromClause from = new OFromClause(-1);
    OFromItem fromItem = new OFromItem(-1);
    fromItem.setIdentifier(new OIdentifier(targetClass));
    from.setItem(fromItem);
    prefetchStm.setTarget(from);
    return prefetchStm;
  }

  /**
   * sort edges in the order they will be matched
   */
  private List<EdgeTraversal> sortEdges(Map<String, Long> estimatedRootEntries, Pattern pattern, OCommandContext ctx) {
    OQueryStats stats = null;
    if (ctx != null && ctx.getDatabase() != null) {
      stats = OQueryStats.get((ODatabaseDocumentInternal) ctx.getDatabase());
    }
    //TODO use the stats

    List<EdgeTraversal> result = new ArrayList<EdgeTraversal>();

    List<OPair<Long, String>> rootWeights = new ArrayList<OPair<Long, String>>();
    for (Map.Entry<String, Long> root : estimatedRootEntries.entrySet()) {
      rootWeights.add(new OPair<Long, String>(root.getValue(), root.getKey()));
    }
    Collections.sort(rootWeights);

    Set<PatternEdge> traversedEdges = new HashSet<PatternEdge>();
    Set<PatternNode> traversedNodes = new HashSet<PatternNode>();
    List<PatternNode> nextNodes = new ArrayList<PatternNode>();

    while (result.size() < pattern.getNumOfEdges()) {
      for (OPair<Long, String> rootPair : rootWeights) {
        PatternNode root = pattern.get(rootPair.getValue());
        if (root.isOptionalNode()) {
          continue;
        }
        if (!traversedNodes.contains(root)) {
          nextNodes.add(root);
          break;
        }
      }

      if (nextNodes.isEmpty()) {
        break;
      }
      while (!nextNodes.isEmpty()) {
        PatternNode node = nextNodes.remove(0);
        traversedNodes.add(node);
        for (PatternEdge edge : node.out) {
          if (!traversedEdges.contains(edge)) {
            result.add(new EdgeTraversal(edge, true));
            traversedEdges.add(edge);
            if (!traversedNodes.contains(edge.in) && !nextNodes.contains(edge.in)) {
              nextNodes.add(edge.in);
            }
          }
        }
        for (PatternEdge edge : node.in) {
          if (!traversedEdges.contains(edge) && edge.item.isBidirectional()) {
            result.add(new EdgeTraversal(edge, false));
            traversedEdges.add(edge);
            if (!traversedNodes.contains(edge.out) && !nextNodes.contains(edge.out)) {
              nextNodes.add(edge.out);
            }
          }
        }
      }
    }

    return result;
  }

  private void buildPatterns(OCommandContext ctx) {
    if (this.pattern != null) {
      return;
    }
    assignDefaultAliases(this.matchExpressions);
    pattern = new Pattern();
    for (OMatchExpression expr : this.matchExpressions) {
      pattern.addExpression(expr.copy());
    }

    Map<String, OWhereClause> aliasFilters = new LinkedHashMap<String, OWhereClause>();
    Map<String, String> aliasClasses = new LinkedHashMap<String, String>();
    for (OMatchExpression expr : this.matchExpressions) {
      addAliases(expr, aliasFilters, aliasClasses, ctx);
    }

    this.aliasFilters = aliasFilters;
    this.aliasClasses = aliasClasses;

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

  private void addAliases(OMatchExpression expr, Map<String, OWhereClause> aliasFilters, Map<String, String> aliasClasses,
      OCommandContext context) {
    addAliases(expr.getOrigin(), aliasFilters, aliasClasses, context);
    for (OMatchPathItem item : expr.getItems()) {
      if (item.getFilter() != null) {
        addAliases(item.getFilter(), aliasFilters, aliasClasses, context);
      }
    }
  }

  private void addAliases(OMatchFilter matchFilter, Map<String, OWhereClause> aliasFilters, Map<String, String> aliasClasses,
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
                "classes defined for alias " + alias + " (" + clazz + ", " + previousClass + ") are not in the same hierarchy");
          }
          aliasClasses.put(alias, lower);
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

  private Map<String, Long> estimateRootEntries(Map<String, String> aliasClasses, Map<String, OWhereClause> aliasFilters,
      OCommandContext ctx) {
    Set<String> allAliases = new LinkedHashSet<String>();
    allAliases.addAll(aliasClasses.keySet());
    allAliases.addAll(aliasFilters.keySet());

    OSchema schema = ctx.getDatabase().getMetadata().getSchema();

    Map<String, Long> result = new LinkedHashMap<String, Long>();
    for (String alias : allAliases) {
      String className = aliasClasses.get(alias);
      if (className == null) {
        continue;
      }

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
    }
    return result;
  }

}
