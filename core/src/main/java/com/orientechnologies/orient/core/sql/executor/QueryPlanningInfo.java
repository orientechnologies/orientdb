package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.sql.parser.*;
import com.orientechnologies.orient.core.storage.OStorage;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Created by luigidellaquila on 19/06/17. */
public class QueryPlanningInfo {

  protected OTimeout timeout;
  protected boolean distinct = false;
  protected boolean expand = false;

  protected OProjection preAggregateProjection;
  protected OProjection aggregateProjection;
  protected OProjection projection = null;
  protected OProjection projectionAfterOrderBy = null;

  protected OLetClause globalLetClause = null;
  protected boolean globalLetPresent = false;

  protected OLetClause perRecordLetClause = null;

  /**
   * in a sharded execution plan, this maps the single server to the clusters it will be queried for
   * to execute the query.
   */
  protected Map<String, Set<String>> serverToClusters;

  protected Map<String, OSelectExecutionPlan> distributedFetchExecutionPlans;

  /**
   * set to true when the distributedFetchExecutionPlans are aggregated in the main execution plan
   */
  public boolean distributedPlanCreated = false;

  protected OFromClause target;
  protected OWhereClause whereClause;
  protected List<OAndBlock> flattenedWhereClause;
  protected OGroupBy groupBy;
  protected OOrderBy orderBy;
  protected OUnwind unwind;
  protected OSkip skip;
  protected OLimit limit;

  protected boolean orderApplied = false;
  protected boolean projectionsCalculated = false;

  protected OAndBlock ridRangeConditions;
  protected OStorage.LOCKING_STRATEGY lockRecord;

  public QueryPlanningInfo copy() {
    // TODO check what has to be copied and what can be just referenced as it is
    QueryPlanningInfo result = new QueryPlanningInfo();
    result.distinct = this.distinct;
    result.expand = this.expand;
    result.preAggregateProjection = this.preAggregateProjection;
    result.aggregateProjection = this.aggregateProjection;
    result.projection = this.projection;
    result.projectionAfterOrderBy = this.projectionAfterOrderBy;
    result.globalLetClause = this.globalLetClause;
    result.globalLetPresent = this.globalLetPresent;
    result.perRecordLetClause = this.perRecordLetClause;
    result.serverToClusters = this.serverToClusters;

    //    Map<String, OSelectExecutionPlan> distributedFetchExecutionPlans;//TODO!

    result.distributedPlanCreated = this.distributedPlanCreated;
    result.target = this.target;
    result.whereClause = this.whereClause;
    result.flattenedWhereClause = this.flattenedWhereClause;
    result.groupBy = this.groupBy;
    result.orderBy = this.orderBy;
    result.unwind = this.unwind;
    result.skip = this.skip;
    result.limit = this.limit;
    result.orderApplied = this.orderApplied;
    result.projectionsCalculated = this.projectionsCalculated;
    result.ridRangeConditions = this.ridRangeConditions;

    result.lockRecord = this.lockRecord;
    return result;
  }
}
