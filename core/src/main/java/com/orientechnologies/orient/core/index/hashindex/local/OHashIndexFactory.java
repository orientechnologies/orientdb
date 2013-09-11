/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.index.hashindex.local;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.index.engine.OLocalHashTableIndexEngine;
import com.orientechnologies.orient.core.index.engine.OMemoryHashMapIndexEngine;
import com.orientechnologies.orient.core.index.engine.ORemoteIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * 
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OHashIndexFactory implements OIndexFactory {
  private static final Set<String> TYPES;
  static {
    final Set<String> types = new HashSet<String>();
    types.add(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.toString());
    types.add(OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.toString());
    types.add(OClass.INDEX_TYPE.FULLTEXT_HASH_INDEX.toString());
    types.add(OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  /**
   * Index types :
   * <ul>
   * <li>UNIQUE</li>
   * <li>NOTUNIQUE</li>
   * <li>FULLTEXT</li>
   * <li>DICTIONARY</li>
   * </ul>
   */
  public Set<String> getTypes() {
    return TYPES;
  }

  public OIndexInternal<?> createIndex(ODatabaseRecord database, String indexType, String algorithm) throws OConfigurationException {
    OStorage storage = database.getStorage();
    OIndexEngine indexEngine;

    final String storageType = storage.getType();
    if (storageType.equals("memory"))
      indexEngine = new OMemoryHashMapIndexEngine();
    else if (storageType.equals("local") || storageType.equals("plocal"))
      indexEngine = new OLocalHashTableIndexEngine();
    else if (storageType.equals("remote"))
      indexEngine = new ORemoteIndexEngine();
    else
      throw new OIndexException("Unsupported storage type : " + storageType);

    if (OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.toString().equals(indexType))
      return new OIndexUnique(indexType, algorithm, indexEngine);
    else if (OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.toString().equals(indexType))
      return new OIndexNotUnique(indexType, algorithm, indexEngine);
    else if (OClass.INDEX_TYPE.FULLTEXT_HASH_INDEX.toString().equals(indexType))
      return new OIndexFullText(indexType, algorithm, indexEngine);
    else if (OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.toString().equals(indexType))
      return new OIndexDictionary(indexType, algorithm, indexEngine);

    throw new OConfigurationException("Unsupported type : " + indexType);
  }
}
