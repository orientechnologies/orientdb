/* Generated By:JJTree: Do not edit this line. OMatchStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.exception.OErrorCode;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OIterableRecordSource;
import com.orientechnologies.orient.core.sql.filter.OSQLTarget;
import com.orientechnologies.orient.core.sql.query.OBasicResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;

public class OMatchStatement extends OStatement implements OCommandExecutor, OIterableRecordSource {

  String                             DEFAULT_ALIAS_PREFIX = "$ORIENT_DEFAULT_ALIAS_";

  private OSQLAsynchQuery<ODocument> request;

  long                               threshold            = 20;

  class MatchContext {
    int                        currentEdgeNumber = 0;

    Map<String, Iterable>      candidates        = new LinkedHashMap<String, Iterable>();
    Map<String, OIdentifiable> matched           = new LinkedHashMap<String, OIdentifiable>();
    Map<PatternEdge, Boolean>  matchedEdges      = new IdentityHashMap<PatternEdge, Boolean>();

    public MatchContext copy(String alias, OIdentifiable value) {
      MatchContext result = new MatchContext();

      result.candidates.putAll(candidates);
      result.candidates.remove(alias);

      result.matched.putAll(matched);
      result.matched.put(alias, value);

      result.matchedEdges.putAll(matchedEdges);
      return result;
    }

    public ODocument toDoc() {
      ODocument doc = new ODocument();
      doc.fromMap((Map) matched);
      return doc;
    }

  }

  public static class EdgeTraversal {
    boolean     out = true;
    PatternEdge edge;

    public EdgeTraversal(PatternEdge edge, boolean out) {
      this.edge = edge;
      this.out = out;
    }
  }

  public static class MatchExecutionPlan {
    public List<EdgeTraversal> sortedEdges;
    public Map<String, Long>   preFetchedAliases = new HashMap<String, Long>();
    public String              rootAlias;
  }

  public static final String        KEYWORD_MATCH    = "MATCH";
  // parsed data
  protected List<OMatchExpression>  matchExpressions = new ArrayList<OMatchExpression>();
  protected List<OExpression>       returnItems      = new ArrayList<OExpression>();
  protected List<OIdentifier>       returnAliases    = new ArrayList<OIdentifier>();
  protected OLimit                  limit;

  protected Pattern                 pattern;

  private Map<String, OWhereClause> aliasFilters;
  private Map<String, String>       aliasClasses;

  // execution data
  private OCommandContext           context;
  private OProgressListener         progressListener;

  public OMatchStatement() {
    super(-1);
  }

  public OMatchStatement(int id) {
    super(id);
  }

  public OMatchStatement(OrientSql p, int id) {
    super(p, id);
  }

  /**
   * Accept the visitor. *
   */
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  // ------------------------------------------------------------------
  // query parsing and optimization
  // ------------------------------------------------------------------

  /**
   * this method parses the statement
   *
   * @param iRequest
   *          Command request implementation.
   *
   * @param <RET>
   * @return
   */
  @Override
  public <RET extends OCommandExecutor> RET parse(OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;
    if (iRequest instanceof OSQLSynchQuery) {
      request = (OSQLSynchQuery<ODocument>) iRequest;
    } else if (iRequest instanceof OSQLAsynchQuery) {
      request = (OSQLAsynchQuery<ODocument>) iRequest;
    } else {
      // BUILD A QUERY OBJECT FROM THE COMMAND REQUEST
      request = new OSQLSynchQuery<ODocument>(textRequest.getText());
      if (textRequest.getResultListener() != null) {
        request.setResultListener(textRequest.getResultListener());
      }
    }
    String queryText = textRequest.getText();

    // please, do not look at this... refactor this ASAP with new executor structure
    final InputStream is = new ByteArrayInputStream(queryText.getBytes());
    final OrientSql osql = new OrientSql(is);
    try {
      OMatchStatement result = (OMatchStatement) osql.parse();
      this.matchExpressions = result.matchExpressions;
      this.returnItems = result.returnItems;
      this.returnAliases = result.returnAliases;
      this.limit = result.limit;
    } catch (ParseException e) {
      OErrorCode.QUERY_PARSE_ERROR.throwException(e.getMessage(), e);
    }

    assignDefaultAliases(this.matchExpressions);
    pattern = new Pattern();
    for (OMatchExpression expr : this.matchExpressions) {
      pattern.addExpression(expr);
    }

    Map<String, OWhereClause> aliasFilters = new LinkedHashMap<String, OWhereClause>();
    Map<String, String> aliasClasses = new LinkedHashMap<String, String>();
    for (OMatchExpression expr : this.matchExpressions) {
      addAliases(expr, aliasFilters, aliasClasses, context);
    }

    this.aliasFilters = aliasFilters;
    this.aliasClasses = aliasClasses;

    rebindFilters(aliasFilters);

    return (RET) this;
  }

  /**
   * rebinds filter (where) conditions to alias nodes after optimization
   * 
   * @param aliasFilters
   */
  private void rebindFilters(Map<String, OWhereClause> aliasFilters) {
    for (OMatchExpression expression : matchExpressions) {
      OWhereClause newFilter = aliasFilters.get(expression.origin.getAlias());
      expression.origin.setFilter(newFilter);

      for (OMatchPathItem item : expression.items) {
        newFilter = aliasFilters.get(item.filter.getAlias());
        item.filter.setFilter(newFilter);
      }
    }
  }

  /**
   * assigns default aliases to pattern nodes that do not have an explicit alias
   * 
   * @param matchExpressions
   */
  private void assignDefaultAliases(List<OMatchExpression> matchExpressions) {
    int counter = 0;
    for (OMatchExpression expression : matchExpressions) {
      if (expression.origin.getAlias() == null) {
        expression.origin.setAlias(DEFAULT_ALIAS_PREFIX + (counter++));
      }

      for (OMatchPathItem item : expression.items) {
        if (item.filter == null) {
          item.filter = new OMatchFilter(-1);
        }
        if (item.filter.getAlias() == null) {
          item.filter.setAlias(DEFAULT_ALIAS_PREFIX + (counter++));
        }
      }
    }
  }

  // ------------------------------------------------------------------
  // query execution
  // ------------------------------------------------------------------

  /**
   * this method works statefully, using request and context variables from current Match statement. This method will be deprecated
   * in next releases
   *
   * @param iArgs
   *          Optional variable arguments to pass to the command.
   *
   * @return
   */
  @Override
  public Object execute(Map<Object, Object> iArgs) {
    this.context.setInputParameters(iArgs);
    return execute(this.request, this.context);
  }

  /**
   * executes the match statement. This is the preferred execute() method and it has to be used as the default one in the future.
   * This method works in stateless mode
   * 
   * @param request
   * @param context
   * @return
   */
  public Object execute(OSQLAsynchQuery<ODocument> request, OCommandContext context) {
    Map<Object, Object> iArgs = context.getInputParameters();
    try {

      Map<String, Long> estimatedRootEntries = estimateRootEntries(aliasClasses, aliasFilters, context);
      if (estimatedRootEntries.values().contains(0l)) {
        return new OBasicResultSet();// some aliases do not match on any classes
      }

      List<EdgeTraversal> sortedEdges = sortEdges(estimatedRootEntries, pattern);
      MatchExecutionPlan executionPlan = new MatchExecutionPlan();
      executionPlan.sortedEdges = sortedEdges;

      calculateMatch(pattern, estimatedRootEntries, new MatchContext(), aliasClasses, aliasFilters, context, request, executionPlan);

      return getResult(request);
    } finally {
      if (request.getResultListener() != null) {
        request.getResultListener().end();
      }
    }

  }

  /**
   * sort edges in the order they will be matched
   */
  private List<EdgeTraversal> sortEdges(Map<String, Long> estimatedRootEntries, Pattern pattern) {
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
        if (!traversedNodes.contains(root)) {
          nextNodes.add(root);
          break;
        }
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

  protected Object getResult(OSQLAsynchQuery<ODocument> request) {
    if (request instanceof OSQLSynchQuery)
      return ((OSQLSynchQuery<ODocument>) request).getResult();

    return null;
  }

  private boolean calculateMatch(Pattern pattern, Map<String, Long> estimatedRootEntries, MatchContext matchContext,
      Map<String, String> aliasClasses, Map<String, OWhereClause> aliasFilters, OCommandContext iCommandContext,
      OSQLAsynchQuery<ODocument> request, MatchExecutionPlan executionPlan) {

    boolean rootFound = false;
    // find starting nodes with few entries
    for (Map.Entry<String, Long> entryPoint : estimatedRootEntries.entrySet()) {
      if (entryPoint.getValue() < threshold) {
        String nextAlias = entryPoint.getKey();
        Iterable<OIdentifiable> matches = fetchAliasCandidates(nextAlias, aliasFilters, iCommandContext, aliasClasses);

        Set<OIdentifiable> ids = new HashSet<OIdentifiable>();
        if (!matches.iterator().hasNext()) {
          return true;
        }

        matchContext.candidates.put(nextAlias, matches);
        executionPlan.preFetchedAliases.put(nextAlias, entryPoint.getValue());
        rootFound = true;
      }
    }
    // no nodes under threshold, guess the smallest one
    if (!rootFound) {
      String nextAlias = getNextAlias(estimatedRootEntries, matchContext);
      Iterable<OIdentifiable> matches = fetchAliasCandidates(nextAlias, aliasFilters, iCommandContext, aliasClasses);
      if (!matches.iterator().hasNext()) {
        return true;
      }
      matchContext.candidates.put(nextAlias, matches);
      executionPlan.preFetchedAliases.put(nextAlias, estimatedRootEntries.get(nextAlias));
    }

    // pick first edge (as sorted before)
    EdgeTraversal firstEdge = executionPlan.sortedEdges.size() == 0 ? null : executionPlan.sortedEdges.get(0);
    String smallestAlias = null;
    // and choose the most convenient starting point (the most convenient traversal direction)
    if (firstEdge != null) {
      smallestAlias = firstEdge.out ? firstEdge.edge.out.alias : firstEdge.edge.in.alias;
    } else {
      smallestAlias = pattern.aliasToNode.values().iterator().next().alias;
    }
    executionPlan.rootAlias = smallestAlias;
    Iterable<OIdentifiable> allCandidates = matchContext.candidates.get(smallestAlias);

    for (OIdentifiable id : allCandidates) {
      MatchContext childContext = matchContext.copy(smallestAlias, id);
      childContext.currentEdgeNumber = 0;
      if (!processContext(pattern, executionPlan, childContext, aliasClasses, aliasFilters, iCommandContext, request)) {
        return false;
      }
    }
    return true;
  }

  private Iterable<OIdentifiable> fetchAliasCandidates(String nextAlias, Map<String, OWhereClause> aliasFilters,
      OCommandContext iCommandContext, Map<String, String> aliasClasses) {
    Iterator<OIdentifiable> it = query(aliasClasses.get(nextAlias), aliasFilters.get(nextAlias), iCommandContext);
    Set<OIdentifiable> result = new HashSet<OIdentifiable>();
    while (it.hasNext()) {
      result.add(it.next().getIdentity());
    }

    return result;
  }

  private boolean processContext(Pattern pattern, MatchExecutionPlan executionPlan, MatchContext matchContext,
      Map<String, String> aliasClasses, Map<String, OWhereClause> aliasFilters, OCommandContext iCommandContext,
      OSQLAsynchQuery<ODocument> request) {

    iCommandContext.setVariable("$matched", matchContext.matched);

    if (pattern.getNumOfEdges() == matchContext.matchedEdges.size() && allNodesCalculated(matchContext, pattern)) {
      // false if limit reached
      return addResult(matchContext, request, iCommandContext);
    }
    if (executionPlan.sortedEdges.size() == matchContext.currentEdgeNumber) {
      // false if limit reached
      return expandCartesianProduct(pattern, matchContext, aliasClasses, aliasFilters, iCommandContext, request);
    }
    EdgeTraversal currentEdge = executionPlan.sortedEdges.get(matchContext.currentEdgeNumber);
    PatternNode rootNode = currentEdge.out ? currentEdge.edge.out : currentEdge.edge.in;

    if (currentEdge.out) {
      PatternEdge outEdge = currentEdge.edge;

      if (!matchContext.matchedEdges.containsKey(outEdge)) {

        Object rightValues = outEdge
            .executeTraversal(matchContext, iCommandContext, matchContext.matched.get(outEdge.out.alias), 0);
        if (!(rightValues instanceof Iterable)) {
          rightValues = Collections.singleton(rightValues);
        }
        for (OIdentifiable rightValue : (Iterable<OIdentifiable>) rightValues) {
          Iterable<OIdentifiable> prevMatchedRightValues = matchContext.candidates.get(outEdge.in.alias);

          if (matchContext.matched.containsKey(outEdge.in.alias)) {
            if (matchContext.matched.get(outEdge.in.alias).getIdentity().equals(rightValue.getIdentity())) {
              MatchContext childContext = matchContext.copy(outEdge.in.alias, rightValue.getIdentity());
              childContext.currentEdgeNumber = matchContext.currentEdgeNumber + 1;
              childContext.matchedEdges.put(outEdge, true);
              if (!processContext(pattern, executionPlan, childContext, aliasClasses, aliasFilters, iCommandContext, request)) {
                return false;
              }
              break;
            }
          } else if (prevMatchedRightValues != null && prevMatchedRightValues.iterator().hasNext()) {// just matching against
                                                                                                     // known
            // values
            for (OIdentifiable id : prevMatchedRightValues) {
              if (id.getIdentity().equals(rightValue.getIdentity())) {
                MatchContext childContext = matchContext.copy(outEdge.in.alias, id);
                childContext.currentEdgeNumber = matchContext.currentEdgeNumber + 1;
                childContext.matchedEdges.put(outEdge, true);
                if (!processContext(pattern, executionPlan, childContext, aliasClasses, aliasFilters, iCommandContext, request)) {
                  return false;
                }
              }
            }
          } else {// searching for neighbors
            MatchContext childContext = matchContext.copy(outEdge.in.alias, rightValue.getIdentity());
            childContext.currentEdgeNumber = matchContext.currentEdgeNumber + 1;
            childContext.matchedEdges.put(outEdge, true);
            if (!processContext(pattern, executionPlan, childContext, aliasClasses, aliasFilters, iCommandContext, request)) {
              return false;
            }
          }
        }
      }
    } else {
      PatternEdge inEdge = currentEdge.edge;
      if (!matchContext.matchedEdges.containsKey(inEdge)) {
        if (!inEdge.item.isBidirectional()) {
          throw new RuntimeException("Invalid pattern to match!");
        }
        if (!matchContext.matchedEdges.containsKey(inEdge)) {
          Object leftValues = inEdge.item.method.executeReverse(matchContext.matched.get(inEdge.in.alias), iCommandContext);
          if (!(leftValues instanceof Iterable)) {
            leftValues = Collections.singleton(leftValues);
          }
          for (OIdentifiable leftValue : (Iterable<OIdentifiable>) leftValues) {
            Iterable<OIdentifiable> prevMatchedRightValues = matchContext.candidates.get(inEdge.out.alias);

            if (matchContext.matched.containsKey(inEdge.out.alias)) {
              if (matchContext.matched.get(inEdge.out.alias).getIdentity().equals(leftValue.getIdentity())) {
                MatchContext childContext = matchContext.copy(inEdge.out.alias, leftValue.getIdentity());
                childContext.currentEdgeNumber = matchContext.currentEdgeNumber + 1;
                childContext.matchedEdges.put(inEdge, true);
                if (!processContext(pattern, executionPlan, childContext, aliasClasses, aliasFilters, iCommandContext, request)) {
                  return false;
                }
                break;
              }
            } else if (prevMatchedRightValues != null && prevMatchedRightValues.iterator().hasNext()) {// just matching against
              // known
              // values
              for (OIdentifiable id : prevMatchedRightValues) {
                if (id.getIdentity().equals(leftValue.getIdentity())) {
                  MatchContext childContext = matchContext.copy(inEdge.out.alias, id);
                  childContext.currentEdgeNumber = matchContext.currentEdgeNumber + 1;
                  childContext.matchedEdges.put(inEdge, true);

                  if (!processContext(pattern, executionPlan, childContext, aliasClasses, aliasFilters, iCommandContext, request)) {
                    return false;
                  }
                }
              }
            } else {// searching for neighbors
              OWhereClause where = aliasFilters.get(inEdge.out.alias);
              if (where == null || where.matchesFilters(leftValue, iCommandContext)) {
                MatchContext childContext = matchContext.copy(inEdge.out.alias, leftValue.getIdentity());
                childContext.currentEdgeNumber = matchContext.currentEdgeNumber + 1;
                childContext.matchedEdges.put(inEdge, true);
                if (!processContext(pattern, executionPlan, childContext, aliasClasses, aliasFilters, iCommandContext, request)) {
                  return false;
                }
              }
            }
          }
        }
      }
    }
    return true;
  }

  private boolean expandCartesianProduct(Pattern pattern, MatchContext matchContext, Map<String, String> aliasClasses,
      Map<String, OWhereClause> aliasFilters, OCommandContext iCommandContext, OSQLAsynchQuery<ODocument> request) {
    for (String alias : pattern.aliasToNode.keySet()) {
      if (!matchContext.matched.containsKey(alias)) {
        String target = aliasClasses.get(alias);
        if (target == null) {
          throw new OCommandExecutionException("Cannot execute MATCH statement on alias " + alias + ": class not defined");
        }

        Iterable<OIdentifiable> values = fetchAliasCandidates(alias, aliasFilters, iCommandContext, aliasClasses);
        for (OIdentifiable id : values) {
          MatchContext childContext = matchContext.copy(alias, id);
          if (allNodesCalculated(childContext, pattern)) {
            // false if limit reached
            boolean added = addResult(childContext, request, iCommandContext);
            if (!added) {
              return false;
            }
          } else {
            // false if limit reached
            boolean added = expandCartesianProduct(pattern, childContext, aliasClasses, aliasFilters, iCommandContext, request);
            if (!added) {
              return false;
            }
          }
        }
        break;
      }
    }
    return true;
  }

  private boolean allNodesCalculated(MatchContext matchContext, Pattern pattern) {
    for (String alias : pattern.aliasToNode.keySet()) {
      if (!matchContext.matched.containsKey(alias)) {
        return false;
      }
    }
    return true;
  }

  private boolean addResult(MatchContext matchContext, OSQLAsynchQuery<ODocument> request, OCommandContext ctx) {

    ODocument doc = null;
    if (returnsMatches()) {
      doc = getDatabase().newInstance();
      for (Map.Entry<String, OIdentifiable> entry : matchContext.matched.entrySet()) {
        if (isExplicitAlias(entry.getKey())) {
          doc.field(entry.getKey(), entry.getValue());
        }
      }
    } else if (returnsPaths()) {
      doc = getDatabase().newInstance();
      for (Map.Entry<String, OIdentifiable> entry : matchContext.matched.entrySet()) {
        doc.field(entry.getKey(), entry.getValue());
      }
    } else if (returnsJson()) {
      doc = jsonToDoc(matchContext, ctx);
    } else {
      doc = getDatabase().newInstance();
      int i = 0;
      for (OExpression item : returnItems) {
        OIdentifier returnAliasIdentifier = returnAliases.get(i);
        String returnAlias;

        if (returnAliasIdentifier == null) {
          returnAlias = item.getDefaultAlias();
        } else {
          returnAlias = returnAliasIdentifier.getValue();
        }
        ODocument mapDoc = new ODocument();
        mapDoc.fromMap((Map) matchContext.matched);
        doc.field(returnAlias, item.execute(mapDoc, ctx));

        i++;
      }
    }


    if (request.getResultListener() != null && doc != null) {
      if(((OBasicCommandContext) context).addToUniqueResult(doc)){
        request.getResultListener().result(doc);
        long currentCount = ((OBasicCommandContext) ctx).getResultsProcessed().incrementAndGet();
        if (limit != null) {
          long limitValue = limit.num.getValue().longValue();
          if (limitValue > -1 && limitValue <= currentCount) {
            return false;
          }
        }
      }
    }

    return true;
  }

  private boolean returnsJson() {
    if (returnItems.size() == 1 && (returnItems.get(0).value instanceof OJson) && returnAliases.get(0) == null) {
      return true;
    }
    return false;
  }

  private ODocument jsonToDoc(MatchContext matchContext, OCommandContext ctx) {
    if (returnItems.size() == 1 && (returnItems.get(0).value instanceof OJson) && returnAliases.get(0) == null) {
      ODocument result = new ODocument();
      result.fromMap(((OJson) returnItems.get(0).value).toMap(matchContext.toDoc(), ctx));
      return result;
    }
    throw new IllegalStateException("Match RETURN statement is not a plain JSON");
  }

  private boolean isExplicitAlias(String key) {
    if (key.startsWith(DEFAULT_ALIAS_PREFIX)) {
      return false;
    }
    return true;
  }

  private boolean returnsMatches() {
    for (OExpression item : returnItems) {
      if (item.toString().equals("$matches")) {
        return true;
      }
    }
    return false;
  }

  private boolean returnsPaths() {
    for (OExpression item : returnItems) {
      if (item.toString().equals("$paths")) {
        return true;
      }
    }
    return false;
  }

  private Iterator<OIdentifiable> query(String className, OWhereClause oWhereClause, OCommandContext ctx) {
    final ODatabaseDocument database = getDatabase();
    OClass schemaClass = database.getMetadata().getSchema().getClass(className);
    database.checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_READ, schemaClass.getName().toLowerCase());

    Iterable<ORecord> baseIterable = fetchFromIndex(schemaClass, oWhereClause);

    // OSelectStatement stm = buildSelectStatement(className, oWhereClause);
    // return stm.execute(ctx);

    String text;
    if (oWhereClause == null) {
      text = "(select from " + className + ")";
    } else {
      StringBuilder builder = new StringBuilder();
      oWhereClause.toString(ctx.getInputParameters(), builder);
      text = "(select from " + className + " where " + builder.toString() + ")";
    }
    OSQLTarget target = new OSQLTarget(text, ctx, "where");

    Iterable targetResult = (Iterable) target.getTargetRecords();
    if (targetResult == null) {
      return null;
    }
    return targetResult.iterator();
  }

  private OSelectStatement buildSelectStatement(String className, OWhereClause oWhereClause) {
    OSelectStatement stm = new OSelectStatement(-1);
    stm.whereClause = oWhereClause;
    stm.target = new OFromClause(-1);
    stm.target.item = new OFromItem(-1);
    stm.target.item.identifier = new OBaseIdentifier(-1);
    stm.target.item.identifier.suffix = new OSuffixIdentifier(-1);
    stm.target.item.identifier.suffix.identifier = new OIdentifier(-1);
    stm.target.item.identifier.suffix.identifier.value = className;
    return stm;
  }

  private Iterable<ORecord> fetchFromIndex(OClass schemaClass, OWhereClause oWhereClause) {
    return null;// TODO
  }

  private String getNextAlias(Map<String, Long> estimatedRootEntries, MatchContext matchContext) {
    Map.Entry<String, Long> lowerValue = null;
    for (Map.Entry<String, Long> entry : estimatedRootEntries.entrySet()) {
      if (matchContext.matched.containsKey(entry.getKey())) {
        continue;
      }
      if (lowerValue == null) {
        lowerValue = entry;
      } else if (lowerValue.getValue() > entry.getValue()) {
        lowerValue = entry;
      }
    }

    return lowerValue.getKey();
  }

  private Map<String, Long> estimateRootEntries(Map<String, String> aliasClasses, Map<String, OWhereClause> aliasFilters,
      OCommandContext ctx) {
    Set<String> allAliases = new LinkedHashSet<String>();
    allAliases.addAll(aliasClasses.keySet());
    allAliases.addAll(aliasFilters.keySet());

    OSchema schema = getDatabase().getMetadata().getSchema();

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

  private void addAliases(OMatchExpression expr, Map<String, OWhereClause> aliasFilters, Map<String, String> aliasClasses,
      OCommandContext context) {
    addAliases(expr.origin, aliasFilters, aliasClasses, context);
    for (OMatchPathItem item : expr.items) {
      if (item.filter != null) {
        addAliases(item.filter, aliasFilters, aliasClasses, context);
      }
    }
  }

  private void addAliases(OMatchFilter matchFilter, Map<String, OWhereClause> aliasFilters, Map<String, String> aliasClasses,
      OCommandContext context) {
    String alias = matchFilter.getAlias();
    OWhereClause filter = matchFilter.getFilter();
    if (alias != null) {
      if (filter != null && filter.baseExpression != null) {
        OWhereClause previousFilter = aliasFilters.get(alias);
        if (previousFilter == null) {
          previousFilter = new OWhereClause(-1);
          previousFilter.baseExpression = new OAndBlock(-1);
          aliasFilters.put(alias, previousFilter);
        }
        OAndBlock filterBlock = (OAndBlock) previousFilter.baseExpression;
        if (filter != null && filter.baseExpression != null) {
          filterBlock.subBlocks.add(filter.baseExpression);
        }
      }

      String clazz = matchFilter.getClassName(context);
      if (clazz != null) {
        String previousClass = aliasClasses.get(alias);
        if (previousClass == null) {
          aliasClasses.put(alias, clazz);
        } else {
          String lower = getLowerSubclass(clazz, previousClass);
          if (lower == null) {
            throw new OCommandExecutionException("classes defined for alias " + alias + " (" + clazz + ", " + previousClass
                + ") are not in the same hierarchy");
          }
          aliasClasses.put(alias, lower);
        }
      }
    }
  }

  private String getLowerSubclass(String className1, String className2) {
    OSchema schema = getDatabase().getMetadata().getSchema();
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

  @Override
  public <RET extends OCommandExecutor> RET setProgressListener(OProgressListener progressListener) {
    this.progressListener = progressListener;
    return (RET) this;
  }

  @Override
  public <RET extends OCommandExecutor> RET setLimit(int iLimit) {
    if (this.limit == null) {
      this.limit = new OLimit(-1);
      this.limit.num = new OInteger(-1);
      this.limit.num.value = iLimit;
    }
    return (RET) this;
  }

  @Override
  public String getFetchPlan() {
    return null;
  }

  @Override
  public Map<Object, Object> getParameters() {
    return null;
  }

  @Override
  public OCommandContext getContext() {
    return context;
  }

  @Override
  public void setContext(OCommandContext context) {
    this.context = context;
  }

  @Override
  public boolean isIdempotent() {
    return true;
  }

  @Override
  public Set<String> getInvolvedClusters() {
    return Collections.EMPTY_SET;
  }

  @Override
  public int getSecurityOperationType() {
    return ORole.PERMISSION_READ;
  }

  @Override
  public boolean involveSchema() {
    return false;
  }

  @Override
  public String getSyntax() {
    return "MATCH <match-statement> [, <match-statement] RETURN <alias>[, <alias>]";
  }

  @Override
  public boolean isLocalExecution() {
    return true;
  }

  @Override
  public boolean isCacheable() {
    return false;
  }

  @Override
  public long getDistributedTimeout() {
    return -1;
  }

  @Override
  public Object mergeResults(Map<String, Object> results) throws Exception {
    return results;
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append(KEYWORD_MATCH);
    builder.append(" ");
    boolean first = true;
    for (OMatchExpression expr : this.matchExpressions) {
      if (!first) {
        builder.append(", ");
      }
      expr.toString(params, builder);
      first = false;
    }
    builder.append(" RETURN ");
    first = true;
    for (OExpression expr : this.returnItems) {
      if (!first) {
        builder.append(", ");
      }
      expr.toString(params, builder);
      first = false;
    }
    if (limit != null) {
      limit.toString(params, builder);
    }
  }

  @Override
  public Iterator<OIdentifiable> iterator(Map<Object, Object> iArgs) {
    if (context == null) {
      context = new OBasicCommandContext();
    }
    Object result = execute(iArgs);
    return ((Iterable) result).iterator();
  }
}
/* JavaCC - OriginalChecksum=6ff0afbe9d31f08b72159fcf24070c9f (do not edit this line) */
