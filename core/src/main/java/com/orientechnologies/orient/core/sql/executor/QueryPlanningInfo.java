package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.sql.parser.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by luigidellaquila on 19/06/17.
 */
public class QueryPlanningInfo {

  boolean distinct = false;
  boolean expand   = false;

  OProjection preAggregateProjection;
  OProjection aggregateProjection;
  OProjection projection             = null;
  OProjection projectionAfterOrderBy = null;

  OLetClause globalLetClause  = null;
  boolean    globalLetPresent = false;

  OLetClause perRecordLetClause = null;

  /**
   * in a sharded execution plan, this maps the single server to the clusters it will be queried for to execute the query.
   */
  Map<String, Set<String>> serverToClusters;

  Map<String, OSelectExecutionPlan> distributedFetchExecutionPlans;

  /**
   * set to true when the distributedFetchExecutionPlans are aggregated in the main execution plan
   */
  public boolean distributedPlanCreated = false;

  OFromClause     target;
  OWhereClause    whereClause;
  List<OAndBlock> flattenedWhereClause;
  OGroupBy        groupBy;
  OOrderBy        orderBy;
  OUnwind         unwind;
  OSkip           skip;
  OLimit          limit;

  boolean orderApplied          = false;
  boolean projectionsCalculated = false;


}
