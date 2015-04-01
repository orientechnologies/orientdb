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
package com.orientechnologies.orient.core.index;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.engine.OSBTreeIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

/**
 * Default OrientDB index factory for indexes based on MVRBTree.<br>
 * Supports index types :
 * <ul>
 * <li>UNIQUE</li>
 * <li>NOTUNIQUE</li>
 * <li>FULLTEXT</li>
 * <li>DICTIONARY</li>
 * </ul>
 */
public class ODefaultIndexFactory implements OIndexFactory {

  public static final String       SBTREE_ALGORITHM             = "SBTREE";

  public static final String       SBTREEBONSAI_VALUE_CONTAINER = "SBTREEBONSAISET";
  public static final String       NONE_VALUE_CONTAINER         = "NONE";

  private static final Set<String> TYPES;
  private static final Set<String> ALGORITHMS;

  static {
    final Set<String> types = new HashSet<String>();
    types.add(OClass.INDEX_TYPE.UNIQUE.toString());
    types.add(OClass.INDEX_TYPE.NOTUNIQUE.toString());
    types.add(OClass.INDEX_TYPE.FULLTEXT.toString());
    types.add(OClass.INDEX_TYPE.DICTIONARY.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  static {
    final Set<String> algorithms = new HashSet<String>();
    algorithms.add(SBTREE_ALGORITHM);
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

  public Set<String> getAlgorithms() {
    return ALGORITHMS;
  }

  public OIndexInternal<?> createIndex(ODatabaseDocumentInternal database, String indexType, String algorithm,
      String valueContainerAlgorithm, ODocument metadata) throws OConfigurationException {
    if (valueContainerAlgorithm == null)
      valueContainerAlgorithm = NONE_VALUE_CONTAINER;

    if (SBTREE_ALGORITHM.equals(algorithm))
      return createSBTreeIndex(indexType, valueContainerAlgorithm, metadata, (OAbstractPaginatedStorage) database.getStorage()
          .getUnderlying());

    throw new OConfigurationException("Unsupported type : " + indexType);
  }

  private OIndexInternal<?> createSBTreeIndex(String indexType, String valueContainerAlgorithm, ODocument metadata,
      OAbstractPaginatedStorage storage) {
    Boolean durableInNonTxMode;

    Object durable = null;

    if (metadata != null) {
      durable = metadata.field("durableInNonTxMode");
    }

    if (durable instanceof Boolean)
      durableInNonTxMode = (Boolean) durable;
    else
      durableInNonTxMode = null;

    if (OClass.INDEX_TYPE.UNIQUE.toString().equals(indexType)) {
      return new OIndexUnique(indexType, SBTREE_ALGORITHM, new OSBTreeIndexEngine<OIdentifiable>(durableInNonTxMode, storage),
          valueContainerAlgorithm, metadata);
    } else if (OClass.INDEX_TYPE.NOTUNIQUE.toString().equals(indexType)) {
      return new OIndexNotUnique(indexType, SBTREE_ALGORITHM, new OSBTreeIndexEngine<Set<OIdentifiable>>(durableInNonTxMode,
          storage), valueContainerAlgorithm, metadata);
    } else if (OClass.INDEX_TYPE.FULLTEXT.toString().equals(indexType)) {
      return new OIndexFullText(indexType, SBTREE_ALGORITHM,
          new OSBTreeIndexEngine<Set<OIdentifiable>>(durableInNonTxMode, storage), valueContainerAlgorithm, metadata);
    } else if (OClass.INDEX_TYPE.DICTIONARY.toString().equals(indexType)) {
      return new OIndexDictionary(indexType, SBTREE_ALGORITHM, new OSBTreeIndexEngine<OIdentifiable>(durableInNonTxMode, storage),
          valueContainerAlgorithm, metadata);
    }

    throw new OConfigurationException("Unsupported type : " + indexType);
  }
}
