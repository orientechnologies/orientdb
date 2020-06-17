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

package com.orientechnologies.orient.core.fetch;

import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OFetchPlan {
  private static final String ANY_WILDCARD = "*";

  private final Map<String, OFetchPlanLevel> fetchPlan = new HashMap<String, OFetchPlanLevel>();
  private final Map<String, OFetchPlanLevel> fetchPlanStartsWith =
      new HashMap<String, OFetchPlanLevel>();

  private static class OFetchPlanLevel {
    public int depthLevelFrom;
    public int depthLevelTo;
    public int level;

    public OFetchPlanLevel(final int iFrom, final int iTo, final int iLevel) {
      depthLevelFrom = iFrom;
      depthLevelTo = iTo;
      level = iLevel;
    }
  }

  public OFetchPlan(final String iFetchPlan) {
    fetchPlan.put(ANY_WILDCARD, new OFetchPlanLevel(0, 0, 0));

    if (iFetchPlan != null && !iFetchPlan.isEmpty()) {
      // CHECK IF THERE IS SOME FETCH-DEPTH
      final List<String> planParts = OStringSerializerHelper.split(iFetchPlan, ' ');
      if (!planParts.isEmpty()) {
        for (String planPart : planParts) {
          final List<String> parts = OStringSerializerHelper.split(planPart, ':');
          if (parts.size() != 2) {
            throw new IllegalArgumentException("Wrong fetch plan: " + planPart);
          }

          String key = parts.get(0);
          final int level = Integer.parseInt(parts.get(1));

          final OFetchPlanLevel fp;

          if (key.startsWith("[")) {
            // EXTRACT DEPTH LEVEL
            final int endLevel = key.indexOf("]");
            if (endLevel == -1)
              throw new IllegalArgumentException(
                  "Missing closing square bracket on depth level in fetch plan: " + key);

            final String range = key.substring(1, endLevel);
            key = key.substring(endLevel + 1);

            if (key.indexOf(".") > -1)
              throw new IllegalArgumentException(
                  "Nested levels (fields separated by dot) are not allowed on fetch plan when dynamic depth level is specified (square brackets): "
                      + key);

            final List<String> indexRanges = OStringSerializerHelper.smartSplit(range, '-', ' ');
            if (indexRanges.size() > 1) {
              // MULTI VALUES RANGE
              String from = indexRanges.get(0);
              String to = indexRanges.get(1);

              final int rangeFrom = from != null && !from.isEmpty() ? Integer.parseInt(from) : 0;
              final int rangeTo = to != null && !to.isEmpty() ? Integer.parseInt(to) : -1;

              fp = new OFetchPlanLevel(rangeFrom, rangeTo, level);
            } else if (range.equals("*"))
              // CREATE FETCH PLAN WITH INFINITE DEPTH
              fp = new OFetchPlanLevel(0, -1, level);
            else {
              // CREATE FETCH PLAN WITH ONE LEVEL ONLY OF DEPTH
              final int v = Integer.parseInt(range);
              fp = new OFetchPlanLevel(v, v, level);
            }
          } else {
            if (level == -1)
              // CREATE FETCH PLAN FOR INFINITE LEVEL
              fp = new OFetchPlanLevel(0, -1, level);
            else
              // CREATE FETCH PLAN FOR FIRST LEVEL ONLY
              fp = new OFetchPlanLevel(0, 0, level);
          }

          if (key.length() > 1 && key.endsWith(ANY_WILDCARD)) {
            fetchPlanStartsWith.put(key.substring(0, key.length() - 1), fp);
          } else {
            fetchPlan.put(key, fp);
          }
        }
      }
    }
  }

  public int getDepthLevel(final String iFieldPath, final int iCurrentLevel) {
    final OFetchPlanLevel value = fetchPlan.get(ANY_WILDCARD);
    final Integer defDepthLevel = value.level;

    final String[] fpParts = iFieldPath.split("\\.");

    for (Map.Entry<String, OFetchPlanLevel> fpLevel : fetchPlan.entrySet()) {
      final String fpLevelKey = fpLevel.getKey();
      final OFetchPlanLevel fpLevelValue = fpLevel.getValue();

      if (iCurrentLevel >= fpLevelValue.depthLevelFrom
          && (fpLevelValue.depthLevelTo == -1 || iCurrentLevel <= fpLevelValue.depthLevelTo)) {
        // IT'S IN RANGE
        if (iFieldPath.equals(fpLevelKey))
          // GET THE FETCH PLAN FOR THE GENERIC FIELD IF SPECIFIED
          return fpLevelValue.level;
        else if (fpLevelKey.startsWith(iFieldPath))
          // SETS THE FETCH LEVEL TO 1 (LOADS ALL DOCUMENT FIELDS)
          return 1;

        for (int i = 0; i < fpParts.length; ++i) {
          if (i >= fpLevelValue.depthLevelFrom
              && (fpLevelValue.depthLevelTo == -1 || i <= fpLevelValue.depthLevelTo)) {
            // IT'S IN RANGE
            if (fpParts[i].equals(fpLevelKey))
              // GET THE FETCH PLAN FOR THE GENERIC FIELD IF SPECIFIED
              return fpLevelValue.level;
          }
        }
      } else {
        if (iFieldPath.equals(fpLevelKey))
          // GET THE FETCH PLAN FOR THE GENERIC FIELD IF SPECIFIED
          return fpLevelValue.level;
        else if (fpLevelKey.startsWith(iFieldPath))
          // SETS THE FETCH LEVEL TO 1 (LOADS ALL DOCUMENT FIELDS)
          return 1;
      }
    }

    if (!fetchPlanStartsWith.isEmpty()) {
      for (Map.Entry<String, OFetchPlanLevel> fpLevel : fetchPlanStartsWith.entrySet()) {
        final String fpLevelKey = fpLevel.getKey();
        final OFetchPlanLevel fpLevelValue = fpLevel.getValue();

        if (iCurrentLevel >= fpLevelValue.depthLevelFrom
            && (fpLevelValue.depthLevelTo == -1 || iCurrentLevel <= fpLevelValue.depthLevelTo)) {
          // IT'S IN RANGE
          for (int i = 0; i < fpParts.length; ++i) {
            if (fpParts[i].startsWith(fpLevelKey)) return fpLevelValue.level;
          }
        }
      }
    }

    return defDepthLevel.intValue();
  }

  public boolean has(final String iFieldPath, final int iCurrentLevel) {
    final String[] fpParts = iFieldPath.split("\\.");

    for (Map.Entry<String, OFetchPlanLevel> fpLevel : fetchPlan.entrySet()) {
      final String fpLevelKey = fpLevel.getKey();
      final OFetchPlanLevel fpLevelValue = fpLevel.getValue();

      if (iCurrentLevel >= fpLevelValue.depthLevelFrom
          && (fpLevelValue.depthLevelTo == -1 || iCurrentLevel <= fpLevelValue.depthLevelTo)) {
        if (iFieldPath.equals(fpLevelKey))
          // GET THE FETCH PLAN FOR THE GENERIC FIELD IF SPECIFIED
          return true;
        else if (fpLevelKey.startsWith(iFieldPath))
          // SETS THE FETCH LEVEL TO 1 (LOADS ALL DOCUMENT FIELDS)
          return true;

        for (int i = 0; i < fpParts.length; ++i) {
          if (i >= fpLevelValue.depthLevelFrom
              && (fpLevelValue.depthLevelTo == -1 || i <= fpLevelValue.depthLevelTo)) {
            // IT'S IN RANGE
            if (fpParts[i].equals(fpLevelKey))
              // GET THE FETCH PLAN FOR THE GENERIC FIELD IF SPECIFIED
              return true;
          }
        }
      } else {
        if (iFieldPath.equals(fpLevelKey))
          // GET THE FETCH PLAN FOR THE GENERIC FIELD IF SPECIFIED
          return true;
        else if (fpLevelKey.startsWith(iFieldPath))
          // SETS THE FETCH LEVEL TO 1 (LOADS ALL DOCUMENT FIELDS)
          return true;
      }
    }

    if (!fetchPlanStartsWith.isEmpty()) {
      for (Map.Entry<String, OFetchPlanLevel> fpLevel : fetchPlanStartsWith.entrySet()) {
        final String fpLevelKey = fpLevel.getKey();
        final OFetchPlanLevel fpLevelValue = fpLevel.getValue();

        if (iCurrentLevel >= fpLevelValue.depthLevelFrom
            && (fpLevelValue.depthLevelTo == -1 || iCurrentLevel <= fpLevelValue.depthLevelTo)) {
          // IT'S IN RANGE
          for (int i = 0; i < fpParts.length; ++i) {
            if (fpParts[i].startsWith(fpLevelKey)) return true;
          }
        }
      }
    }
    return false;
  }
}
