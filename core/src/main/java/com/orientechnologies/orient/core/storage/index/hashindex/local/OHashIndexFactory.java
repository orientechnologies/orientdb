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
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.engine.OHashTableIndexEngine;
import com.orientechnologies.orient.core.storage.index.engine.ORemoteIndexEngine;
import com.orientechnologies.orient.core.storage.rocks.ORocksIndexEngine;
import com.orientechnologies.orient.core.storage.rocks.ORocksStorage;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OHashIndexFactory implements OIndexFactory {

  private static final Set<String> TYPES;
  public static final String HASH_INDEX_ALGORITHM = "HASH_INDEX";
  private static final Set<String> ALGORITHMS;

  static {
    final Set<String> types = new HashSet<String>();
    types.add(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.toString());
    types.add(OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.toString());
    types.add(OClass.INDEX_TYPE.FULLTEXT_HASH_INDEX.toString());
    types.add(OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  static {
    final Set<String> algorithms = new HashSet<String>();
    algorithms.add(HASH_INDEX_ALGORITHM);

    ALGORITHMS = Collections.unmodifiableSet(algorithms);
  }

  /**
   * Index types : <ul> <li>UNIQUE</li> <li>NOTUNIQUE</li> <li>FULLTEXT</li> <li>DICTIONARY</li> </ul>
   */
  public Set<String> getTypes() {
    return TYPES;
  }

  public Set<String> getAlgorithms() {
    return ALGORITHMS;
  }

  public OIndexInternal<?> createIndex(String name, OStorage storage, String indexType, String algorithm,
      String valueContainerAlgorithm, ODocument metadata, int version) throws OConfigurationException {

    if (version < 0)
      version = getLastVersion();

    if (valueContainerAlgorithm == null)
      valueContainerAlgorithm = ODefaultIndexFactory.NONE_VALUE_CONTAINER;

    if (OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.toString().equals(indexType))
      return new OIndexUnique(name, indexType, algorithm, version, storage.getUnderlying(), valueContainerAlgorithm, metadata);
    else if (OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.toString().equals(indexType))
      return new OIndexNotUnique(name, indexType, algorithm, version, storage.getUnderlying(), valueContainerAlgorithm, metadata);
    else if (OClass.INDEX_TYPE.FULLTEXT_HASH_INDEX.toString().equals(indexType))
      return new OIndexFullText(name, indexType, algorithm, version, storage.getUnderlying(), valueContainerAlgorithm, metadata);
    else if (OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.toString().equals(indexType))
      return new OIndexDictionary(name, indexType, algorithm, version, storage.getUnderlying(), valueContainerAlgorithm, metadata);

    throw new OConfigurationException("Unsupported type: " + indexType);
  }

  @Override
  public int getLastVersion() {
    return OHashTableIndexEngine.VERSION;
  }

  @Override
  public OIndexEngine createIndexEngine(final String algoritm, final String name, final Boolean durableInNonTxMode,
      final OStorage storage, final int version, final Map<String, String> engineProperties) {
    OIndexEngine indexEngine;

    final String storageType = storage.getType();
    if (storageType.equals("memory") || storageType.equals("plocal"))
      indexEngine = new OHashTableIndexEngine(name, durableInNonTxMode, (OAbstractPaginatedStorage) storage, version);
    else if (storageType.equals("distributed"))
      // DISTRIBUTED CASE: HANDLE IT AS FOR LOCAL
      indexEngine = new OHashTableIndexEngine(name, durableInNonTxMode, (OAbstractPaginatedStorage) storage.getUnderlying(), version);
    else if (storageType.equals("remote"))
      indexEngine = new ORemoteIndexEngine(name);
    else if (storageType.equals("rocks"))
      indexEngine = new ORocksIndexEngine((ORocksStorage) storage, name);
    else
      throw new OIndexException("Unsupported storage type: " + storageType);

    return indexEngine;
  }
}
