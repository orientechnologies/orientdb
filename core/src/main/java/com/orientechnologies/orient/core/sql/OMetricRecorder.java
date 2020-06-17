/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.index.OIndex;
import java.util.HashSet;
import java.util.Set;

/** @author Artem Orobets (enisher-at-gmail.com) */
public class OMetricRecorder {
  protected OCommandContext context;

  public void setContext(OCommandContext context) {
    this.context = context;
  }

  public void recordOrderByOptimizationMetric(
      boolean indexIsUsedInOrderBy, boolean fullySortedByIndex) {
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
      } else idxNames.add(index.getName());
    }
  }

  OCommandContext orderByElapsed(long startOrderBy) {
    return context.setVariable("orderByElapsed", (System.currentTimeMillis() - startOrderBy));
  }

  public void recordRangeQueryConvertedInBetween() {
    if (context.isRecordingMetrics()) {
      Integer counter = (Integer) context.getVariable("rangeQueryConvertedInBetween");
      if (counter == null) counter = 0;

      counter++;
      context.setVariable("rangeQueryConvertedInBetween", counter);
    }
  }
}
