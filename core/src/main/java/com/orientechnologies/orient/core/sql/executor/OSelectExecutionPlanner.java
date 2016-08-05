package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Luigi Dell'Aquila
 */
public class OSelectExecutionPlanner {

  private boolean distinct = false;
  private boolean expand   = false;

  private OProjection preAggregateProjection;
  private OProjection aggregateProjection;
  private OProjection projection = null;

  private OLetClause globalLetClause    = null;
  private OLetClause perRecordLetClause = null;

  private OFromClause     target;
  private OWhereClause    whereClause;
  private List<OAndBlock> flattenedWhereClause;
  private OGroupBy        groupBy;
  private OOrderBy        orderBy;
  private OUnwind         unwind;
  private OSkip           skip;
  private OLimit          limit;

  private boolean orderApplied          = false;
  private boolean projectionsCalculated = false;

  public OSelectExecutionPlanner(OSelectStatement oSelectStatement) {
    //copying the content, so that it can be manipulated and optimized
    this.projection = oSelectStatement.getProjection() == null ? null : oSelectStatement.getProjection().copy();
    translateDistinct(this.projection);
    this.distinct = projection == null ? false : projection.isDistinct();
    if (projection != null) {
      this.projection.setDistinct(false);
    }

    this.target = oSelectStatement.getTarget();
    this.whereClause = oSelectStatement.getWhereClause() == null ? null : oSelectStatement.getWhereClause().copy();
    this.perRecordLetClause = oSelectStatement.getLetClause() == null ? null : oSelectStatement.getLetClause().copy();
    this.groupBy = oSelectStatement.getGroupBy() == null ? null : oSelectStatement.getGroupBy().copy();
    this.orderBy = oSelectStatement.getOrderBy() == null ? null : oSelectStatement.getOrderBy().copy();
    this.unwind = oSelectStatement.getUnwind() == null ? null : oSelectStatement.getUnwind().copy();
    this.skip = oSelectStatement.getSkip();
    this.limit = oSelectStatement.getLimit();
  }

  /**
   * for backward compatibility, translate "distinct(foo)" to "DISTINCT foo".
   * This method modifies the projection itself.
   *
   * @param projection the projection
   */
  private void translateDistinct(OProjection projection) {
    //TODO
  }

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx) {
    OSelectExecutionPlan result = new OSelectExecutionPlan(ctx);

    if (expand && distinct) {
      throw new OCommandExecutionException("Cannot execute a statement with DISTINCT expand(), please use a subquery");
    }

    optimizeQuery();

    handleGlobalLet(result, globalLetClause, ctx);
    handleFetchFromTarger(result, ctx);
    handleLet(result, perRecordLetClause, ctx);

    handleWhere(result, whereClause, ctx);

    if (expand || unwind != null) {
      handleProjectionsBeforeOrderBy(result, projection, orderBy, ctx);
      handleProjections(result, ctx);
      handleExpand(result, ctx);
      handleUnwind(result, unwind, ctx);
      handleOrderBy(result, orderBy, ctx);
      if (skip != null) {
        result.chain(new SkipExecutionStep(skip, ctx));
      }
      if (limit != null) {
        result.chain(new LimitExecutionStep(limit, ctx));
      }
    } else {
      handleProjectionsBeforeOrderBy(result, projection, orderBy, ctx);
      handleOrderBy(result, orderBy, ctx);
      if (distinct) {
        handleProjections(result, ctx);
        handleDistinct(result, ctx);
        if (skip != null) {
          result.chain(new SkipExecutionStep(skip, ctx));
        }
        if (limit != null) {
          result.chain(new LimitExecutionStep(limit, ctx));
        }
      } else {
        if (skip != null) {
          result.chain(new SkipExecutionStep(skip, ctx));
        }
        if (limit != null) {
          result.chain(new LimitExecutionStep(limit, ctx));
        }
        handleProjections(result, ctx);
      }
    }
    return result;
  }

  private void handleUnwind(OSelectExecutionPlan result, OUnwind unwind, OCommandContext ctx) {
    if (unwind != null) {
      result.chain(new UnwindStep(unwind, ctx));
    }
  }

  private void handleDistinct(OSelectExecutionPlan result, OCommandContext ctx) {
    result.chain(new DistinctExecutionStep(ctx));
  }

  private void handleProjectionsBeforeOrderBy(OSelectExecutionPlan result, OProjection projection, OOrderBy orderBy,
      OCommandContext ctx) {
    if (orderBy != null) {
      handleProjections(result, ctx);
    }
  }

  private void handleProjections(OSelectExecutionPlan result, OCommandContext ctx) {
    if (!this.projectionsCalculated && projection != null) {
      if (preAggregateProjection != null) {
        result.chain(new ProjectionCalculationStep(preAggregateProjection, ctx));
      }
      if (aggregateProjection != null) {
        result.chain(new AggregateProjectionCalculationStep(aggregateProjection, groupBy, ctx));
      }
      result.chain(new ProjectionCalculationStep(projection, ctx));

      this.projectionsCalculated = true;
    }
  }

  private void optimizeQuery() {
    splitLet();
    extractSubQueries();
    if (projection != null && this.projection.isExpand()) {
      expand = true;
      this.projection = projection.getExpandContent();
    }
    if (whereClause != null) {
      flattenedWhereClause = whereClause.flatten();
      //this helps index optimization
      flattenedWhereClause = moveFlattededEqualitiesLeft(flattenedWhereClause);
    }

    extractAggregateProjections();
    addOrderByProjections();
  }

  /**
   * splits LET clauses in global (executed once) and local (executed once per record)
   */
  private void splitLet() {
    if (this.perRecordLetClause != null && this.perRecordLetClause.getItems() != null) {
      Iterator<OLetItem> iterator = this.perRecordLetClause.getItems().iterator();
      while (iterator.hasNext()) {
        OLetItem item = iterator.next();
        if (item.getExpression() != null && item.getExpression().isEarlyCalculated()) {
          iterator.remove();
          addGlobalLet(item.getVarName(), item.getExpression());
        } else if (item.getQuery() != null && !item.getQuery().refersToParent()) {
          iterator.remove();
          addGlobalLet(item.getVarName(), item.getQuery());
        }
      }
    }
  }

  /**
   * re-writes a list of flat AND conditions, moving left all the equality operations
   *
   * @param flattenedWhereClause
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

  private void addOrderByProjections() {
    if (orderApplied || orderBy == null || orderBy.getItems().size() == 0 || projection == null || projection.getItems() == null
        || (projection.getItems().size() == 1 && projection.getItems().get(0).isAll())) {
      return;
    }
    //TODO
  }

  private void extractAggregateProjections() {
    if (projection == null) {
      return;
    }

    OProjection preAggregate = new OProjection(-1);
    preAggregate.setItems(new ArrayList<>());
    OProjection aggregate = new OProjection(-1);
    aggregate.setItems(new ArrayList<>());
    OProjection postAggregate = new OProjection(-1);
    postAggregate.setItems(new ArrayList<>());

    boolean isAggregate = false;
    AggregateProjectionSplit result = new AggregateProjectionSplit();
    for (OProjectionItem item : this.projection.getItems()) {
      result.reset();
      if (item.isAggregate()) {
        isAggregate = true;
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
    if (isAggregate) {
      this.preAggregateProjection = preAggregate;
      if (preAggregateProjection.getItems() == null || preAggregateProjection.getItems().size() == 0) {
        preAggregateProjection = null;
      }
      this.aggregateProjection = aggregate;
      if (aggregateProjection.getItems() == null || aggregateProjection.getItems().size() == 0) {
        aggregateProjection = null;
      }
      this.projection = postAggregate;

      addGroupByExpressionsToProjections();
    }
  }

  /**
   * if GROUP BY is performed on an expression that is not explicitly in the pre-aggregate projections, then
   * that expression has to be put in the pre-aggregate (only here, in subsequent steps it's removed)
   */
  private void addGroupByExpressionsToProjections() {
    if (this.groupBy == null || this.groupBy.getItems() == null || this.groupBy.getItems().size() == 0) {
      return;
    }
    OGroupBy newGroupBy = new OGroupBy(-1);
    int i = 0;
    for (OExpression exp : groupBy.getItems()) {
      if (exp.isAggregate()) {
        throw new OCommandExecutionException("Cannot group by an aggregate function");
      }
      boolean found = false;
      for (String alias : preAggregateProjection.getAllAliases()) {
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
        preAggregateProjection.getItems().add(newItem);
        newGroupBy.getItems().add(new OExpression(groupByAlias));
      }

      groupBy = newGroupBy;
    }

  }

  /**
   * translates subqueries to LET statements
   */
  private void extractSubQueries() {
    SubQueryCollector collector = new SubQueryCollector();
    if (perRecordLetClause != null) {
      perRecordLetClause.extractSubQueries(collector);
    }
    int i = 0;
    int j = 0;
    for (Map.Entry<OIdentifier, OStatement> entry : collector.getSubQueries().entrySet()) {
      OIdentifier alias = entry.getKey();
      OStatement query = entry.getValue();
      if (query.refersToParent()) {
        addRecordLevelLet(alias, query, j++);
      } else {
        addGlobalLet(alias, query, i++);
      }
    }
    collector.reset();

    if (whereClause != null) {
      whereClause.extractSubQueries(collector);
    }
    if (projection != null) {
      projection.extractSubQueries(collector);
    }
    if (orderBy != null) {
      orderBy.extractSubQueries(collector);
    }
    if (groupBy != null) {
      groupBy.extractSubQueries(collector);
    }

    for (Map.Entry<OIdentifier, OStatement> entry : collector.getSubQueries().entrySet()) {
      OIdentifier alias = entry.getKey();
      OStatement query = entry.getValue();
      if (query.refersToParent()) {
        addRecordLevelLet(alias, query);
      } else {
        addGlobalLet(alias, query);
      }
    }
  }

  private void addGlobalLet(OIdentifier alias, OExpression exp) {
    if (globalLetClause == null) {
      globalLetClause = new OLetClause(-1);
    }
    OLetItem item = new OLetItem(-1);
    item.setVarName(alias);
    item.setExpression(exp);
    globalLetClause.addItem(item);
  }

  private void addGlobalLet(OIdentifier alias, OStatement stm) {
    if (globalLetClause == null) {
      globalLetClause = new OLetClause(-1);
    }
    OLetItem item = new OLetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    globalLetClause.addItem(item);
  }

  private void addGlobalLet(OIdentifier alias, OStatement stm, int pos) {
    if (globalLetClause == null) {
      globalLetClause = new OLetClause(-1);
    }
    OLetItem item = new OLetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    globalLetClause.getItems().add(pos, item);
  }

  private void addRecordLevelLet(OIdentifier alias, OStatement stm) {
    if (perRecordLetClause == null) {
      perRecordLetClause = new OLetClause(-1);
    }
    OLetItem item = new OLetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    perRecordLetClause.addItem(item);
  }

  private void addRecordLevelLet(OIdentifier alias, OStatement stm, int pos) {
    if (perRecordLetClause == null) {
      perRecordLetClause = new OLetClause(-1);
    }
    OLetItem item = new OLetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    perRecordLetClause.getItems().add(pos, item);
  }

  private void handleFetchFromTarger(OSelectExecutionPlan result, OCommandContext ctx) {

    OFromItem target = this.target == null ? null : this.target.getItem();
    if (target == null) {
      handleNoTarget(result, ctx);
    } else if (target.getIdentifier() != null) {
      handleClassAsTarget(result, target.getIdentifier(), ctx);
    } else if (target.getCluster() != null) {
      handleClustersAsTarget(result, Collections.singletonList(target.getCluster()), ctx);
    } else if (target.getClusterList() != null) {
      handleClustersAsTarget(result, target.getClusterList().toListOfClusters(), ctx);
    } else if (target.getStatement() != null) {
      handleSubqueryAsTarget(result, target.getStatement(), ctx);
    } else if (target.getFunctionCall() != null) {
      //        handleFunctionCallAsTarget(result, target.getFunctionCall(), ctx);//TODO
    } else if (target.getInputParam() != null) {
      //        handleInputParamAsTarget(result, target.getInputParam(), ctx);//TODO
    } else if (target.getIndex() != null) {
      handleIndexAsTarget(result, target.getIndex(), whereClause, orderBy, ctx);
    } else if (target.getMetadata() != null) {
      handleMetadataAsTarget(result, target.getMetadata(), ctx);
    } else if (target.getRids() != null && target.getRids().size() > 0) {
      handleRidsAsTarget(result, target.getRids(), ctx);
    } else {
      throw new UnsupportedOperationException();
    }

  }

  private void handleNoTarget(OSelectExecutionPlan result, OCommandContext ctx) {
    result.chain(new EmptyDataGeneratorStep(1, ctx));
  }

  private void handleIndexAsTarget(OSelectExecutionPlan result, OIndexIdentifier indexIdentifier, OWhereClause whereClause,
      OOrderBy orderBy, OCommandContext ctx) {
    String indexName = indexIdentifier.getIndexName();
    OIndex<?> index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(indexName);
    if (index == null) {
      throw new OCommandExecutionException("Index not found: " + indexName);
    }

    switch (indexIdentifier.getType()) {
    case INDEX:
      OBooleanExpression condition = null;
      if (flattenedWhereClause == null || flattenedWhereClause.size() == 0) {
        if (!index.supportsOrderedIterations()) {
          throw new OCommandExecutionException("Index " + indexName + " does not allow iteration without a condition");
        }
      } else if (flattenedWhereClause.size() > 1) {
        throw new OCommandExecutionException("Index queries with this kind of condition are not supported yet: " + whereClause);
      } else {
        OAndBlock andBlock = flattenedWhereClause.get(0);
        if (andBlock.getSubBlocks().size() != 1) {
          throw new OCommandExecutionException("Index queries with this kind of condition are not supported yet: " + whereClause);
        }
        this.whereClause = null;//The WHERE clause won't be used anymore, the index does all the filtering
        condition = andBlock.getSubBlocks().get(0);
      }
      result.chain(new FetchFromIndexStep(index, condition, null, ctx));
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

  private void handleExpand(OSelectExecutionPlan result, OCommandContext ctx) {
    if (expand) {
      result.chain(new ExpandStep(ctx));
    }
  }

  private void handleGlobalLet(OSelectExecutionPlan result, OLetClause letClause, OCommandContext ctx) {
    if (letClause != null) {
      List<OLetItem> items = letClause.getItems();
      for (OLetItem item : items) {
        if (item.getExpression() != null) {
          result.chain(new GlobalLetExpressionStep(item.getVarName(), item.getExpression(), ctx));
        } else {
          result.chain(new GlobalLetQueryStep(item.getVarName(), item.getQuery(), ctx));
        }
      }
    }
  }

  private void handleLet(OSelectExecutionPlan result, OLetClause letClause, OCommandContext ctx) {
    if (letClause != null) {
      List<OLetItem> items = letClause.getItems();
      for (OLetItem item : items) {
        if (item.getExpression() != null) {
          result.chain(new LetExpressionStep(item.getVarName(), item.getExpression(), ctx));
        } else {
          result.chain(new LetQueryStep(item.getVarName(), item.getQuery(), ctx));
        }
      }
    }
  }

  private void handleWhere(OSelectExecutionPlan plan, OWhereClause whereClause, OCommandContext ctx) {
    if (whereClause != null) {
      plan.chain(new FilterStep(whereClause, ctx));
    }
  }

  private void handleOrderBy(OSelectExecutionPlan plan, OOrderBy orderBy, OCommandContext ctx) {
    //TODO skip and limit in line
    if (!orderApplied && orderBy != null && orderBy.getItems() != null && orderBy.getItems().size() > 0) {
      plan.chain(new OrderByStep(orderBy, ctx));
    }
  }

  private void handleClassAsTarget(OSelectExecutionPlan plan, OIdentifier identifier, OCommandContext ctx) {
    if (handleClassAsTargetWithIndex(plan, identifier, ctx)) {
      return;
    }

    Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    if (isOrderByRidAsc()) {
      orderByRidAsc = true;
    } else if (isOrderByRidDesc()) {
      orderByRidAsc = false;
    }
    FetchFromClassExecutionStep fetcher = new FetchFromClassExecutionStep(identifier.getStringValue(), ctx, orderByRidAsc);
    if (orderByRidAsc != null) {
      this.orderApplied = true;
    }
    plan.chain(fetcher);
  }

  private boolean handleClassAsTargetWithIndex(OSelectExecutionPlan plan, OIdentifier targetClass, OCommandContext ctx) {
    List<OExecutionStepInternal> result = handleSubclassAsTargetWithIndex(targetClass.getStringValue(), ctx);
    if (result != null) {
      result.stream().forEach(x -> plan.chain(x));
      this.whereClause = null;
      this.flattenedWhereClause = null;
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
      List<OExecutionStepInternal> subSteps = handleSubclassAsTargetWithIndexDeep(subClass.getName(), ctx);
      if (subSteps == null || subSteps.size() == 0) {
        return false;
      }
      OSelectExecutionPlan subPlan = new OSelectExecutionPlan(ctx);
      subSteps.stream().forEach(x -> subPlan.chain(x));
      subclassPlans.add(subPlan);
    }
    if (subclassPlans.size() > 0) {
      plan.chain(new ParallelExecStep(subclassPlans, ctx));
      return true;
    }
    return false;
  }

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

  private List<OExecutionStepInternal> handleSubclassAsTargetWithIndexDeep(String targetClass, OCommandContext ctx) {
    List<OExecutionStepInternal> result = handleSubclassAsTargetWithIndex(targetClass, ctx);
    if (result == null) {
      result = new ArrayList<>();
      //TODO recursive on subclassess
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
        List<OExecutionStepInternal> subSteps = handleSubclassAsTargetWithIndexDeep(subClass.getName(), ctx);
        if (subSteps == null || subSteps.size() == 0) {
          return null;
        }
        OSelectExecutionPlan subPlan = new OSelectExecutionPlan(ctx);
        subSteps.stream().forEach(x -> subPlan.chain(x));
        subclassPlans.add(subPlan);
      }
      if (subclassPlans.size() > 0) {
        result.add(new ParallelExecStep(subclassPlans, ctx));
      }
    }
    return result.size() == 0 ? null : result;
  }

  private List<OExecutionStepInternal> handleSubclassAsTargetWithIndex(String targetClass, OCommandContext ctx) {
    if (flattenedWhereClause == null || flattenedWhereClause.size() == 0) {
      return null;
    }

    //TODO indexable functions!

    OClass clazz = ctx.getDatabase().getMetadata().getSchema().getClass(targetClass);
    if (clazz == null) {
      throw new OCommandExecutionException("Cannot find class " + targetClass);
    }

    Set<OIndex<?>> indexes = clazz.getIndexes();

    List<IndexSearchDescriptor> indexSearchDescriptors = flattenedWhereClause.stream().map(x -> findBestIndexFor(ctx, indexes, x))
        .filter(Objects::nonNull).collect(Collectors.toList());
    if (indexSearchDescriptors.size() != flattenedWhereClause.size()) {
      return null; //some blocks could not be managed with an index
    }

    List<OExecutionStepInternal> result = null;
    List<IndexSearchDescriptor> optimumIndexSearchDescriptors = commonFactor(indexSearchDescriptors);

    if (indexSearchDescriptors.size() == 1) {
      IndexSearchDescriptor desc = indexSearchDescriptors.get(0);
      result = new ArrayList<>();
      result.add(new FetchFromIndexStep(desc.idx, desc.keyCondition, desc.additionalRangeCondition, ctx));
      if (desc.remainingCondition != null && !desc.remainingCondition.isEmpty()) {
        result.add(new FilterStep(createWhereFrom(desc.remainingCondition), ctx));
      }
    } else {
      result = new ArrayList<>();
      result.add(createParallelIndexFetch(optimumIndexSearchDescriptors, ctx));
    }
    return result;
  }

  private OExecutionStepInternal createParallelIndexFetch(List<IndexSearchDescriptor> indexSearchDescriptors, OCommandContext ctx) {
    List<OInternalExecutionPlan> subPlans = new ArrayList<>();
    for (IndexSearchDescriptor desc : indexSearchDescriptors) {
      OSelectExecutionPlan subPlan = new OSelectExecutionPlan(ctx);
      subPlan.chain(new FetchFromIndexStep(desc.idx, desc.keyCondition, desc.additionalRangeCondition, ctx));
      if (desc.remainingCondition != null && !desc.remainingCondition.isEmpty()) {
        subPlan.chain(new FilterStep(createWhereFrom(desc.remainingCondition), ctx));
      }
      subPlans.add(subPlan);
    }
    return new ParallelExecStep(subPlans, ctx);
  }

  private OWhereClause createWhereFrom(OBooleanExpression remainingCondition) {
    OWhereClause result = new OWhereClause(-1);
    result.setBaseExpression(remainingCondition);
    return result;
  }

  /**
   * given a flat AND block and a set of indexes, returns the best index to be used to process it, with the complete descripton on how to use it
   *
   * @param ctx
   * @param indexes
   * @param block
   * @return
   */
  private IndexSearchDescriptor findBestIndexFor(OCommandContext ctx, Set<OIndex<?>> indexes, OAndBlock block) {
    return indexes.stream().map(index -> buildIndexSearchDescriptor(ctx, index, block)).filter(Objects::nonNull)
        .filter(x -> x.keyCondition != null).filter(x -> x.keyCondition.getSubBlocks().size() > 0)
        .min(Comparator.comparing(x -> x.cost(ctx))).orElse(null);
  }

  /**
   * given an index and a flat AND block, returns a descriptor on how to process it with an index (index, index key and additional filters
   * to apply after index fetch
   *
   * @param ctx
   * @param index
   * @param block
   * @return
   */
  private IndexSearchDescriptor buildIndexSearchDescriptor(OCommandContext ctx, OIndex<?> index, OAndBlock block) {
    List<String> indexFields = index.getDefinition().getFields();
    OBinaryCondition keyCondition = new OBinaryCondition(-1);
    OIdentifier key = new OIdentifier(-1);
    key.setStringValue("key");
    keyCondition.setLeft(new OExpression(key));
    boolean allowsRange = allowsRangeQueries(index);
    boolean found = false;

    OAndBlock blockCopy = block.copy();
    Iterator<OBooleanExpression> blockIterator = blockCopy.getSubBlocks().iterator();

    OAndBlock indexKeyValue = new OAndBlock(-1);
    IndexSearchDescriptor result = new IndexSearchDescriptor();
    result.idx = index;
    result.keyCondition = indexKeyValue;
    for (String indexField : indexFields) {
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

  private void handleClustersAsTarget(OSelectExecutionPlan plan, List<OCluster> clusters, OCommandContext ctx) {
    ODatabase db = ctx.getDatabase();
    Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    if (isOrderByRidAsc()) {
      orderByRidAsc = true;
    } else if (isOrderByRidDesc()) {
      orderByRidAsc = false;
    }
    if (orderByRidAsc != null) {
      this.orderApplied = true;
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

  private boolean isOrderByRidDesc() {
    if (!hasTargetWithSortedRids()) {
      return false;
    }

    if (orderBy == null) {
      return false;
    }
    if (orderBy.getItems().size() == 1) {
      OOrderByItem item = orderBy.getItems().get(0);
      String recordAttr = item.getRecordAttr();
      if (recordAttr != null && recordAttr.equalsIgnoreCase("@rid") && OOrderByItem.DESC.equals(item.getType())) {
        return true;
      }
    }
    return false;
  }

  private boolean isOrderByRidAsc() {
    if (!hasTargetWithSortedRids()) {
      return false;
    }

    if (orderBy == null) {
      return false;
    }
    if (orderBy.getItems().size() == 1) {
      OOrderByItem item = orderBy.getItems().get(0);
      String recordAttr = item.getRecordAttr();
      if (recordAttr != null && recordAttr.equalsIgnoreCase("@rid") &&
          (item.getType() == null || OOrderByItem.ASC.equals(item.getType()))) {
        return true;
      }
    }
    return false;
  }

  private boolean hasTargetWithSortedRids() {
    if (target == null) {
      return false;
    }
    if (target.getItem() == null) {
      return false;
    }
    if (target.getItem().getIdentifier() != null) {
      return true;
    } else if (target.getItem().getCluster() != null) {
      return true;
    } else if (target.getItem().getClusterList() != null) {
      return true;
    }
    return false;
  }

}
