/*
 * Copyright 2012 Orient Technologies.
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
package com.orientechnologies.orient.core.sharding;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexFactory;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.index.OIndexNotUnique;
import com.orientechnologies.orient.core.index.OIndexUnique;
import com.orientechnologies.orient.core.index.engine.ORemoteIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Auto-sharding index factory.<br>
 * Supports index types:
 * <ul>
 * <li>UNIQUE</li>
 * <li>NOTUNIQUE</li>
 * </ul>
 * 
 * @since 3.0
 */
public class OAutoShardingIndexFactory implements OIndexFactory {

  public static final String AUTOSHARDING_ALGORITHM = "AUTOSHARDING";
  public static final String NONE_VALUE_CONTAINER   = "NONE";

  private static final Set<String> TYPES;
  private static final Set<String> ALGORITHMS;

  static {
    final Set<String> types = new HashSet<String>();
    types.add(OClass.INDEX_TYPE.UNIQUE.toString());
    types.add(OClass.INDEX_TYPE.NOTUNIQUE.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  static {
    final Set<String> algorithms = new HashSet<String>();
    algorithms.add(AUTOSHARDING_ALGORITHM);
    ALGORITHMS = Collections.unmodifiableSet(algorithms);
  }

  public static boolean isMultiValueIndex(final String indexType) {
    switch (OClass.INDEX_TYPE.valueOf(indexType)) {
    case UNIQUE:
    case UNIQUE_HASH_INDEX:
    case DICTIONARY:
    case DICTIONARY_HASH_INDEX:
      return false;
    }

    return true;
  }

  /**
   * Index types:
   * <ul>
   * <li>UNIQUE</li>
   * <li>NOTUNIQUE</li>
   * </ul>
   */
  public Set<String> getTypes() {
    return TYPES;
  }

  public Set<String> getAlgorithms() {
    return ALGORITHMS;
  }

  public OIndexInternal<?> createIndex(String name, ODatabaseDocumentInternal database, String indexType, String algorithm,
      String valueContainerAlgorithm, ODocument metadata, int version) throws OConfigurationException {
    if (valueContainerAlgorithm == null)
      valueContainerAlgorithm = NONE_VALUE_CONTAINER;

    if (version < 0)
      version = getLastVersion();

    if (AUTOSHARDING_ALGORITHM.equals(algorithm))
      return createShardedIndex(name, indexType, valueContainerAlgorithm, metadata,
          (OAbstractPaginatedStorage) database.getStorage().getUnderlying(), version);

    throw new OConfigurationException("Unsupported type: " + indexType);
  }

  private OIndexInternal<?> createShardedIndex(final String name, final String indexType, final String valueContainerAlgorithm,
      final ODocument metadata, final OAbstractPaginatedStorage storage, final int version) {

    if (OClass.INDEX_TYPE.UNIQUE.toString().equals(indexType)) {
      return new OIndexUnique(name, indexType, AUTOSHARDING_ALGORITHM, version, storage, valueContainerAlgorithm, metadata);
    } else if (OClass.INDEX_TYPE.NOTUNIQUE.toString().equals(indexType)) {
      return new OIndexNotUnique(name, indexType, AUTOSHARDING_ALGORITHM, version, storage, valueContainerAlgorithm, metadata);
    }

    throw new OConfigurationException("Unsupported type: " + indexType);
  }

  @Override
  public int getLastVersion() {
    return OAutoShardingIndexEngine.VERSION;
  }

  @Override
  public OIndexEngine createIndexEngine(final String algorithm, final String name, final Boolean durableInNonTxMode,
      final OStorage storage, final int version, final Map<String, String> engineProperties) {

    final OIndexEngine indexEngine;

    final String storageType = storage.getType();
    if (storageType.equals("memory") || storageType.equals("plocal"))
      indexEngine = new OAutoShardingIndexEngine(name, durableInNonTxMode, (OAbstractPaginatedStorage) storage, version);
    else if (storageType.equals("distributed"))
      // DISTRIBUTED CASE: HANDLE IT AS FOR LOCAL
      indexEngine = new OAutoShardingIndexEngine(name, durableInNonTxMode, (OAbstractPaginatedStorage) storage.getUnderlying(),
          version);
    else
      if (storageType.equals("remote"))
        // MANAGE REMOTE SHARDED INDEX TO CALL THE INTERESTED SERVER
        indexEngine = new ORemoteIndexEngine(name);
    else
      throw new OIndexException("Unsupported storage type: " + storageType);

    return indexEngine;
  }
}
