/*
 * Copyright 2010-2014 OrientDB LTD (info(-at-)orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.metadata.schema.clusterselection;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Returns the cluster selecting the most empty between all configured clusters.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OBalancedClusterSelectionStrategy implements OClusterSelectionStrategy {
  public static final String NAME = "balanced";
  protected static final long REFRESH_TIMEOUT = 5000;
  protected long lastCount = -1;
  protected int smallerClusterId = -1;

  public int getCluster(final OClass iClass, final ODocument doc) {
    return getCluster(iClass, iClass.getClusterIds(), doc);
  }

  public int getCluster(final OClass iClass, final int[] clusters, final ODocument doc) {

    if (clusters.length == 1)
      // ONLY ONE: RETURN THE FIRST ONE
      return clusters[0];

    final ODatabaseDocument db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db == null) return clusters[0];

    if (lastCount < 0 || System.currentTimeMillis() - lastCount > REFRESH_TIMEOUT) {
      // REFRESH COUNTERS
      long min = Long.MAX_VALUE;

      for (int cluster : clusters) {
        final long count = db.countClusterElements(cluster);
        if (count < min) {
          min = count;
          smallerClusterId = cluster;
        }
      }
      lastCount = System.currentTimeMillis();
    }

    return smallerClusterId;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
