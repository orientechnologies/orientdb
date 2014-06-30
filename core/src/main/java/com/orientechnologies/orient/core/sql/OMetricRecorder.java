package com.orientechnologies.orient.core.sql;

import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.index.OIndex;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OMetricRecorder {
  protected OCommandContext context;

  public void setContext(OCommandContext context) {
    this.context = context;
  }

  public void recordOrderByOptimizationMetric(boolean indexIsUsedInOrderBy, boolean fullySortedByIndex) {
    if (context.isRecordingMetrics()) {
      context.setVariable("indexIsUsedInOrderBy", indexIsUsedInOrderBy);
      context.setVariable("fullySortedByIndex", fullySortedByIndex);
    }
  }

  public void recordInvolvedIndexesMetric(OIndex index) {
    if (context.isRecordingMetrics()) {
      Set<String> idxNames = (Set<String>) context.getVariable("involvedIndexes");
      if (idxNames == null) {
        idxNames = new HashSet<String>();
        context.setVariable("involvedIndexes", idxNames);
      }
      if (index instanceof OChainedIndexProxy) {
        idxNames.addAll(((OChainedIndexProxy) index).getIndexNames());
      } else
        idxNames.add(index.getName());
    }
  }

  OCommandContext orderByElapsed(long startOrderBy) {
    return context.setVariable("orderByElapsed", (System.currentTimeMillis() - startOrderBy));
  }
}
