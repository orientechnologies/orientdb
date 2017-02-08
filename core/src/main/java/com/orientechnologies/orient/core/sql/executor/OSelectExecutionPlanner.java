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

  private boolean distinct = false;
  private boolean expand   = false;

  private OProjection preAggregateProjection;
  private OProjection aggregateProjection;
  private OProjection projection             = null;
  private OProjection projectionAfterOrderBy = null;

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
    projection = translateDistinct(this.projection);
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

    splitProjectionsForGroupBy();
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

  /**
   * creates additional projections for ORDER BY
   */
  private void addOrderByProjections() {
    if (orderApplied || expand || unwind != null || orderBy == null || orderBy.getItems().size() == 0 || projection == null
        || projection.getItems() == null || (projection.getItems().size() == 1 && projection.getItems().get(0).isAll())) {
      return;
    }

    OOrderBy newOrderBy = orderBy == null ? null : orderBy.copy();
    List<OProjectionItem> additionalOrderByProjections = calculateAdditionalOrderByProjections(this.projection.getAllAliases(),
        newOrderBy);
    if (additionalOrderByProjections.size() > 0) {
      orderBy = newOrderBy;//the ORDER BY has changed
    }
    if (additionalOrderByProjections.size() > 0) {
      projectionAfterOrderBy = new OProjection(-1);
      projectionAfterOrderBy.setItems(new ArrayList<>());
      for (String alias : projection.getAllAliases()) {
        projectionAfterOrderBy.getItems().add(projectionFromAlias(new OIdentifier(alias)));
      }

      for (OProjectionItem item : additionalOrderByProjections) {
        if (preAggregateProjection != null) {
          preAggregateProjection.getItems().add(item);
          aggregateProjection.getItems().add(projectionFromAlias(item.getAlias()));
          projection.getItems().add(projectionFromAlias(item.getAlias()));
        } else {
          projection.getItems().add(item);
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
   * @return a list of additional projections to add to the existing projections to allow ORDER BY calculation (empty if nothing has to be added).
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
  private void splitProjectionsForGroupBy() {
    if (projection == null) {
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
    for (OProjectionItem item : this.projection.getItems()) {
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
      handleIndexAsTarget(result, target.getIndex(), whereClause, orderBy, ctx);
    } else if (target.getMetadata() != null) {
      handleMetadataAsTarget(result, target.getMetadata(), ctx);
    } else if (target.getRids() != null && target.getRids().size() > 0) {
      handleRidsAsTarget(result, target.getRids(), ctx);
    } else {
      throw new UnsupportedOperationException();
    }

  }

  private void handleInputParamAsTarget(OSelectExecutionPlan result, OInputParameter inputParam, OCommandContext ctx) {
    Object paramValue = inputParam.bindFromInputParams(ctx.getInputParameters());
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
        this.flattenedWhereClause = null;
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
    int skipSize = skip == null ? 0 : skip.getValue(ctx);
    if (skipSize < 0) {
      throw new OCommandExecutionException("Cannot execute a query with a negative SKIP");
    }
    int limitSize = limit == null ? -1 : limit.getValue(ctx);
    Integer maxResults = null;
    if (limitSize >= 0) {
      maxResults = skipSize + limitSize;
    }
    if (expand || unwind != null) {
      maxResults = null;
    }
    if (!orderApplied && orderBy != null && orderBy.getItems() != null && orderBy.getItems().size() > 0) {
      plan.chain(new OrderByStep(orderBy, maxResults, ctx));
      if (projectionAfterOrderBy != null) {
        plan.chain(new ProjectionCalculationStep(projectionAfterOrderBy, ctx));
      }
    }
  }

  private void handleClassAsTarget(OSelectExecutionPlan plan, OFromClause queryTarget, OCommandContext ctx) {
    OIdentifier identifier = queryTarget.getItem().getIdentifier();
    if (handleClassAsTargetWithIndexedFunction(plan, queryTarget, flattenedWhereClause, ctx)) {
      return;
    }
    if (handleClassAsTargetWithIndex(plan, identifier, ctx)) {
      return;
    }

    if (orderBy != null && handleClassWithIndexForSortOnly(plan, identifier, orderBy, ctx)) {
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

  private boolean handleClassAsTargetWithIndexedFunction(OSelectExecutionPlan plan, OFromClause fromClause,
      List<OAndBlock> flattenedWhereClause, OCommandContext ctx) {
    OIdentifier queryTarget = fromClause.getItem().getIdentifier();
    if (queryTarget == null) {
      return false;
    }
    OClass clazz = ctx.getDatabase().getMetadata().getSchema().getClass(queryTarget.getStringValue());
    if (clazz == null) {
      throw new OCommandExecutionException("Class not found: " + queryTarget);
    }
    if (flattenedWhereClause == null || flattenedWhereClause.size() == 0) {
      return false;
    }

    List<OInternalExecutionPlan> resultSubPlans = new ArrayList<>();

    for (OAndBlock block : flattenedWhereClause) {
      List<OBinaryCondition> indexedFunctionConditions = block
          .getIndexedFunctionConditions(clazz, (ODatabaseDocumentInternal) ctx.getDatabase());
      if (indexedFunctionConditions == null || indexedFunctionConditions.size() == 0) {
        return false;//no indexed functions for this block
      } else {
        OBinaryCondition blockCandidateFunction = null;
        for (OBinaryCondition cond : indexedFunctionConditions) {
          if (!cond.allowsIndexedFunctionExecutionOnTarget(fromClause, ctx)) {
            if (!cond.canExecuteIndexedFunctionWithoutIndex(fromClause, ctx)) {
              throw new OCommandExecutionException("Cannot execute " + block + " on " + queryTarget);
            }
          }
          if (blockCandidateFunction == null) {
            blockCandidateFunction = cond;
          } else {
            boolean thisAllowsNoIndex = cond.canExecuteIndexedFunctionWithoutIndex(fromClause, ctx);
            boolean prevAllowsNoIndex = blockCandidateFunction.canExecuteIndexedFunctionWithoutIndex(fromClause, ctx);
            if (!thisAllowsNoIndex && !prevAllowsNoIndex) {
              //none of the functions allow execution without index, so cannot choose one
              throw new OCommandExecutionException(
                  "Cannot choose indexed function between " + cond + " and " + blockCandidateFunction
                      + ". Both require indexed execution");
            } else if (thisAllowsNoIndex && prevAllowsNoIndex) {
              //both can be calculated without index, choose the best one for index execution
              long thisEstimate = cond.estimateIndexed(fromClause, ctx);
              long lastEstimate = blockCandidateFunction.estimateIndexed(fromClause, ctx);
              if (thisEstimate > -1 && thisEstimate < lastEstimate) {
                blockCandidateFunction = cond;
              }
            } else if (prevAllowsNoIndex) {
              //choose current condition, because the other one can be calculated without index
              blockCandidateFunction = cond;
            }
          }
        }

        FetchFromIndexedFunctionStep step = new FetchFromIndexedFunctionStep(blockCandidateFunction, fromClause, ctx);
        if (!blockCandidateFunction.executeIndexedFunctionAfterIndexSearch(fromClause, ctx)) {
          block = block.copy();
          block.getSubBlocks().remove(blockCandidateFunction);
        }
        if (flattenedWhereClause.size() == 1) {
          plan.chain(step);
          if (!block.getSubBlocks().isEmpty()) {
            plan.chain(new FilterStep(createWhereFrom(block), ctx));
          }
        } else {
          OSelectExecutionPlan subPlan = new OSelectExecutionPlan(ctx);
          subPlan.chain(step);
          if (!block.getSubBlocks().isEmpty()) {
            subPlan.chain(new FilterStep(createWhereFrom(block), ctx));
          }
          resultSubPlans.add(subPlan);
        }
      }
    }
    if(resultSubPlans.size()>0) {
      plan.chain(new ParallelExecStep(resultSubPlans, ctx));
    }
    //WHERE condition already applied
    this.whereClause = null;
    this.flattenedWhereClause = null;
    return true;
  }

  /**
   * tries to use an index for sorting only. Also adds the fetch step to the execution plan
   *
   * @param plan        current execution plan
   * @param queryTarget the query target (class)
   * @param orderBy     ORDER BY clause
   * @param ctx         the current context
   * @return true if it succeeded to use an index to sort, false otherwise.
   */

  private boolean handleClassWithIndexForSortOnly(OSelectExecutionPlan plan, OIdentifier queryTarget, OOrderBy orderBy,
      OCommandContext ctx) {
    OClass clazz = ctx.getDatabase().getMetadata().getSchema().getClass(queryTarget.getStringValue());
    if (clazz == null) {
      throw new OCommandExecutionException("Class not found: " + queryTarget.getStringValue());
    }

    for (OIndex idx : clazz.getIndexes().stream().filter(i -> i.supportsOrderedIterations()).filter(i -> i.getDefinition() != null)
        .collect(Collectors.toList())) {
      List<String> indexFields = idx.getDefinition().getFields();
      if (indexFields.size() < orderBy.getItems().size()) {
        continue;
      }
      boolean indexFound = true;
      String orderType = null;
      for (int i = 0; i < orderBy.getItems().size(); i++) {
        OOrderByItem orderItem = orderBy.getItems().get(i);
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
        plan.chain(new FetchFromIndexValuesStep(idx, orderType.equals(OOrderByItem.ASC), ctx));
        orderApplied = true;
        return true;
      }
    }
    return false;
  }

  private boolean handleClassAsTargetWithIndex(OSelectExecutionPlan plan, OIdentifier targetClass, OCommandContext ctx) {
    List<OExecutionStepInternal> result = handleClassAsTargetWithIndex(targetClass.getStringValue(), ctx);
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
      List<OExecutionStepInternal> subSteps = handleClassAsTargetWithIndexRecursive(subClass.getName(), ctx);
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

  private List<OExecutionStepInternal> handleClassAsTargetWithIndexRecursive(String targetClass, OCommandContext ctx) {
    List<OExecutionStepInternal> result = handleClassAsTargetWithIndex(targetClass, ctx);
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
        List<OExecutionStepInternal> subSteps = handleClassAsTargetWithIndexRecursive(subClass.getName(), ctx);
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

  private List<OExecutionStepInternal> handleClassAsTargetWithIndex(String targetClass, OCommandContext ctx) {
    if (flattenedWhereClause == null || flattenedWhereClause.size() == 0) {
      return null;
    }

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
      Boolean orderAsc = getOrderDirection();
      result.add(
          new FetchFromIndexStep(desc.idx, desc.keyCondition, desc.additionalRangeCondition, !Boolean.FALSE.equals(orderAsc), ctx));
      if (orderAsc != null && orderBy != null && fullySorted(orderBy, desc.keyCondition, desc.idx)) {
        orderApplied = true;
      }
      if (desc.remainingCondition != null && !desc.remainingCondition.isEmpty()) {
        result.add(new FilterStep(createWhereFrom(desc.remainingCondition), ctx));
      }
    } else {
      result = new ArrayList<>();
      result.add(createParallelIndexFetch(optimumIndexSearchDescriptors, ctx));
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
      final String orderFieldName = orderedFields.get(i).toLowerCase();//toLowerCase...? remove this?
      final String indexFieldName = fields.get(i).toLowerCase();//toLowerCase...?
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
  private Boolean getOrderDirection() {
    if (orderBy == null) {
      return null;
    }
    String result = null;
    for (OOrderByItem item : orderBy.getItems()) {
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
   * given a flat AND block and a set of indexes, returns the best index to be used to process it, with the complete description on how to use it
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
