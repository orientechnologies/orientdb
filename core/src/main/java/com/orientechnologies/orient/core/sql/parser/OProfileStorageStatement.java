/* Generated By:JJTree: Do not edit this line. OProfileStorageStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;

import java.util.Map;

public class OProfileStorageStatement extends OStatement {

  protected boolean on;

  public static final String KEYWORD_PROFILE = "PROFILE";

  public OProfileStorageStatement(int id) {
    super(id);
  }

  public OProfileStorageStatement(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public Object execute(OSQLAsynchQuery<ODocument> request, OCommandContext context, OProgressListener progressListener) {
    try {
      ODatabaseDocumentInternal db = getDatabase();

      final OStorage storage = db.getStorage();
      if (on) {
        // activate the profiler
        ((OAbstractPaginatedStorage) storage).startGatheringPerformanceStatisticForCurrentThread();
        ODocument result = new ODocument();
        result.field("result", "OK");
        request.getResultListener().result(result);
      } else {
        // stop the profiler and return the stats
        final OSessionStoragePerformanceStatistic performanceStatistic = ((OAbstractPaginatedStorage) storage)
            .completeGatheringPerformanceStatisticForCurrentThread();

        if (performanceStatistic != null)
          request.getResultListener().result(performanceStatistic.toDocument());
        else {
          ODocument result = new ODocument();
          result.field("result", "Error: profiling of storage was not started.");
          request.getResultListener().result(result);
        }

      }
      return getResult(request);
    } finally {
      if (request.getResultListener() != null) {
        request.getResultListener().end();
      }
    }
  }

  protected Object getResult(OSQLAsynchQuery<ODocument> request) {
    if (request instanceof OSQLSynchQuery)
      return ((OSQLSynchQuery<ODocument>) request).getResult();

    return null;
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("PROFILE STORAGE ");
    builder.append(on ? "ON" : "OFF");
  }
}

/* JavaCC - OriginalChecksum=645887712797ae14a17820bfa944f78e (do not edit this line) */
