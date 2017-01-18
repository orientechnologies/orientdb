/*
 * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
package com.orientechnologies.orient.core.sharding.auto;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.util.List;

/**
 * Returns the cluster selecting through the hash function.
 *
 * @since 3.0
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OAutoShardingClusterSelectionStrategy implements OClusterSelectionStrategy {
  public static final String NAME = "auto-sharding";
  private final OIndex       index;
  private final OIndexEngine indexEngine;
  private final List<String> indexedFields;
  private final int[]        clusters;

  public OAutoShardingClusterSelectionStrategy(final OClass clazz, final OIndex autoShardingIndex) {
    index = autoShardingIndex;
    if (index == null)
      throw new OConfigurationException(
          "Cannot use auto-sharding cluster strategy because class '" + clazz + "' has no auto-sharding index defined");

    indexedFields = index.getDefinition().getFields();
    if (indexedFields.size() != 1)
      throw new OConfigurationException("Cannot use auto-sharding cluster strategy because class '" + clazz
          + "' has an auto-sharding index defined with multiple fields");

    final OStorage stg = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying();
    if (!(stg instanceof OAbstractPaginatedStorage))
      throw new OConfigurationException("Cannot use auto-sharding cluster strategy because storage is not embedded");

    try {
      indexEngine = ((OAbstractPaginatedStorage) stg).getIndexEngine(index.getIndexId());
    } catch (OInvalidIndexEngineIdException e) {
      throw new OConfigurationException("Cannot use auto-sharding cluster strategy because the underlying index has not found");
    }

    if (indexEngine == null)
      throw new OConfigurationException("Cannot use auto-sharding cluster strategy because the underlying index has not found");

    clusters = clazz.getClusterIds();
  }

  public int getCluster(final OClass clazz, final ODocument doc) {
    final Object fieldValue = doc.field(indexedFields.get(0));

    return clusters[((OAutoShardingIndexEngine) indexEngine).getStrategy().getPartitionsId(fieldValue, clusters.length)];
  }

  @Override
  public String getName() {
    return NAME;
  }
}
