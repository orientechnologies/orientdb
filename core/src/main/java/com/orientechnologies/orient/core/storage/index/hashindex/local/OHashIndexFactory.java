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
package com.orientechnologies.orient.core.storage.index.hashindex.local;

import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.ODefaultIndexFactory;
import com.orientechnologies.orient.core.index.OIndexDictionary;
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
import com.orientechnologies.orient.core.storage.index.engine.OHashTableIndexEngine;
import com.orientechnologies.orient.core.storage.index.engine.ORemoteIndexEngine;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** @author Artem Orobets (enisher-at-gmail.com) */
public final class OHashIndexFactory implements OIndexFactory {

  private static final Set<String> TYPES;
  public static final String HASH_INDEX_ALGORITHM = "HASH_INDEX";
  private static final Set<String> ALGORITHMS;

  static {
    final Set<String> types = new HashSet<>(4);
    types.add(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.toString());
    types.add(OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.toString());
    types.add(OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  static {
    final Set<String> algorithms = new HashSet<>(1);
    algorithms.add(HASH_INDEX_ALGORITHM);

    ALGORITHMS = Collections.unmodifiableSet(algorithms);
  }

  /**
   * Index types :
   *
   * <ul>
   *   <li>UNIQUE
   *   <li>NOTUNIQUE
   *   <li>FULLTEXT
   *   <li>DICTIONARY
   * </ul>
   */
  public final Set<String> getTypes() {
    return TYPES;
  }

  public final Set<String> getAlgorithms() {
    return ALGORITHMS;
  }

  public final OIndexInternal createIndex(
      final String name,
      final OStorage storage,
      final String indexType,
      final String algorithm,
      String valueContainerAlgorithm,
      final ODocument metadata,
      int version)
      throws OConfigurationException {

    if (version < 0) {
      version = getLastVersion(algorithm);
    }

    if (valueContainerAlgorithm == null)
      valueContainerAlgorithm = ODefaultIndexFactory.NONE_VALUE_CONTAINER;

    final int binaryFormatVersion = storage.getConfiguration().getBinaryFormatVersion();

    if (OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.toString().equals(indexType)) {
      return new OIndexUnique(
          name,
          indexType,
          algorithm,
          version,
          (OAbstractPaginatedStorage) storage,
          valueContainerAlgorithm,
          metadata,
          binaryFormatVersion);
    } else if (OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.toString().equals(indexType)) {
      return new OIndexNotUnique(
          name,
          indexType,
          algorithm,
          version,
          (OAbstractPaginatedStorage) storage,
          valueContainerAlgorithm,
          metadata,
          binaryFormatVersion);
    } else if (OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.toString().equals(indexType)) {
      return new OIndexDictionary(
          name,
          indexType,
          algorithm,
          version,
          (OAbstractPaginatedStorage) storage,
          valueContainerAlgorithm,
          metadata,
          binaryFormatVersion);
    }

    throw new OConfigurationException("Unsupported type: " + indexType);
  }

  @Override
  public final int getLastVersion(final String algorithm) {
    return OHashTableIndexEngine.VERSION;
  }

  @Override
  public final OBaseIndexEngine createIndexEngine(
      final int indexId,
      final String algorithm,
      final String name,
      final OStorage storage,
      final int version,
      final boolean multiValue) {
    final OIndexEngine indexEngine;

    final String storageType = storage.getType();
    switch (storageType) {
      case "memory":
      case "plocal":
        indexEngine =
            new OHashTableIndexEngine(name, indexId, (OAbstractPaginatedStorage) storage, version);
        break;
      case "distributed":
        // DISTRIBUTED CASE: HANDLE IT AS FOR LOCAL
        indexEngine =
            new OHashTableIndexEngine(name, indexId, (OAbstractPaginatedStorage) storage, version);
        break;
      case "remote":
        indexEngine = new ORemoteIndexEngine(indexId, name);
        break;
      default:
        throw new OIndexException("Unsupported storage type: " + storageType);
    }

    return indexEngine;
  }
}
