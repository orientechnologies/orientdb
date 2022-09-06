/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexFactory;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.index.OIndexNotUnique;
import com.orientechnologies.orient.core.index.OIndexUnique;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.index.engine.OIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.engine.ORemoteIndexEngine;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Auto-sharding index factory.<br>
 * Supports index types:
 *
 * <ul>
 *   <li>UNIQUE
 *   <li>NOTUNIQUE
 * </ul>
 *
 * @since 3.0
 */
public class OAutoShardingIndexFactory implements OIndexFactory {

  public static final String AUTOSHARDING_ALGORITHM = "AUTOSHARDING";
  public static final String NONE_VALUE_CONTAINER = "NONE";

  private static final Set<String> TYPES;
  private static final Set<String> ALGORITHMS;

  static {
    final Set<String> types = new HashSet<>();
    types.add(OClass.INDEX_TYPE.UNIQUE.toString());
    types.add(OClass.INDEX_TYPE.NOTUNIQUE.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  static {
    final Set<String> algorithms = new HashSet<>();
    algorithms.add(AUTOSHARDING_ALGORITHM);
    ALGORITHMS = Collections.unmodifiableSet(algorithms);
  }

  /**
   * Index types:
   *
   * <ul>
   *   <li>UNIQUE
   *   <li>NOTUNIQUE
   * </ul>
   */
  public Set<String> getTypes() {
    return TYPES;
  }

  public Set<String> getAlgorithms() {
    return ALGORITHMS;
  }

  public OIndexInternal createIndex(
      String name,
      OStorage storage,
      String indexType,
      String algorithm,
      String valueContainerAlgorithm,
      ODocument metadata,
      int version)
      throws OConfigurationException {
    if (valueContainerAlgorithm == null) valueContainerAlgorithm = NONE_VALUE_CONTAINER;

    if (version < 0) {
      version = getLastVersion(algorithm);
    }

    if (AUTOSHARDING_ALGORITHM.equals(algorithm))
      return createShardedIndex(
          name,
          indexType,
          valueContainerAlgorithm,
          metadata,
          (OAbstractPaginatedStorage) storage,
          version);

    throw new OConfigurationException("Unsupported type: " + indexType);
  }

  private OIndexInternal createShardedIndex(
      final String name,
      final String indexType,
      final String valueContainerAlgorithm,
      final ODocument metadata,
      final OAbstractPaginatedStorage storage,
      final int version) {

    final int binaryFormatVersion = storage.getConfiguration().getBinaryFormatVersion();

    if (OClass.INDEX_TYPE.UNIQUE.toString().equals(indexType)) {
      return new OIndexUnique(
          name,
          indexType,
          AUTOSHARDING_ALGORITHM,
          version,
          storage,
          valueContainerAlgorithm,
          metadata,
          binaryFormatVersion);
    } else if (OClass.INDEX_TYPE.NOTUNIQUE.toString().equals(indexType)) {
      return new OIndexNotUnique(
          name,
          indexType,
          AUTOSHARDING_ALGORITHM,
          version,
          storage,
          valueContainerAlgorithm,
          metadata,
          binaryFormatVersion);
    }

    throw new OConfigurationException("Unsupported type: " + indexType);
  }

  @Override
  public int getLastVersion(final String algorithm) {
    return OAutoShardingIndexEngine.VERSION;
  }

  @Override
  public OBaseIndexEngine createIndexEngine(
      int indexId,
      final String algorithm,
      final String name,
      final OStorage storage,
      final int version,
      boolean multiValue) {

    final OIndexEngine indexEngine;

    final String storageType = storage.getType();
    switch (storageType) {
      case "memory":
      case "plocal":
        indexEngine =
            new OAutoShardingIndexEngine(
                name, indexId, (OAbstractPaginatedStorage) storage, version);
        break;
      case "distributed":
        // DISTRIBUTED CASE: HANDLE IT AS FOR LOCAL
        indexEngine =
            new OAutoShardingIndexEngine(
                name, indexId, (OAbstractPaginatedStorage) storage, version);
        break;
      case "remote":
        // MANAGE REMOTE SHARDED INDEX TO CALL THE INTERESTED SERVER
        indexEngine = new ORemoteIndexEngine(indexId, name);
        break;
      default:
        throw new OIndexException("Unsupported storage type: " + storageType);
    }

    return indexEngine;
  }
}
