package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.sql.parser.*;

import java.util.List;

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

  OLetClause globalLetClause    = null;
  OLetClause perRecordLetClause = null;

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
