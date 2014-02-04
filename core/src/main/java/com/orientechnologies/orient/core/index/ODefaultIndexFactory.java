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
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.engine.local.OEngineLocal;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.engine.OMVRBTreeIndexEngine;
import com.orientechnologies.orient.core.index.engine.OSBTreeIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OClass;

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
  public static final String       MVRBTREE_ALGORITHM           = "MVRBTREE";

  public static final String       MVRBTREE_VALUE_CONTAINER     = "MVRBTREESET";
  public static final String       SBTREEBONSAI_VALUE_CONTAINER = "SBTREEBONSAISET";
  public static final String       NONE_VALUE_CONTAINER         = "NONE";

  private static final Set<String> TYPES;
  static {
    final Set<String> types = new HashSet<String>();
    types.add(OClass.INDEX_TYPE.UNIQUE.toString());
    types.add(OClass.INDEX_TYPE.NOTUNIQUE.toString());
    types.add(OClass.INDEX_TYPE.FULLTEXT.toString());
    types.add(OClass.INDEX_TYPE.DICTIONARY.toString());
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

  public OIndexInternal<?> createIndex(ODatabaseRecord database, String indexType, String algorithm, String valueContainerAlgorithm)
      throws OConfigurationException {
    if (valueContainerAlgorithm == null) {
      if (OClass.INDEX_TYPE.NOTUNIQUE.toString().equals(indexType)
          || OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.toString().equals(indexType)
          || OClass.INDEX_TYPE.FULLTEXT_HASH_INDEX.toString().equals(indexType)
          || OClass.INDEX_TYPE.FULLTEXT.toString().equals(indexType))
        valueContainerAlgorithm = MVRBTREE_VALUE_CONTAINER;
      else
        valueContainerAlgorithm = NONE_VALUE_CONTAINER;
    }

    if ((database.getStorage().getType().equals(OEngineLocalPaginated.NAME) || database.getStorage().getType()
        .equals(OEngineLocal.NAME))
        && valueContainerAlgorithm.equals(ODefaultIndexFactory.MVRBTREE_VALUE_CONTAINER)
        && OGlobalConfiguration.INDEX_NOTUNIQUE_USE_SBTREE_CONTAINER_BY_DEFAULT.getValueAsBoolean()) {
      OLogManager
          .instance()
          .warn(
              this,
              "Index was created using %s as values container. "
                  + "This container is deprecated and is not supported any more. To avoid this message please drop and recreate indexes or perform DB export/import.",
              valueContainerAlgorithm);
    }

    if (SBTREE_ALGORITHM.equals(algorithm))
      return createSBTreeIndex(indexType, valueContainerAlgorithm);

    if (MVRBTREE_ALGORITHM.equals(algorithm) || algorithm == null)
      return createMRBTreeIndex(indexType, valueContainerAlgorithm);

    throw new OConfigurationException("Unsupported type : " + indexType);
  }

  private OIndexInternal<?> createMRBTreeIndex(String indexType, String valueContainerAlgorithm) {
    if (OClass.INDEX_TYPE.UNIQUE.toString().equals(indexType)) {
      return new OIndexUnique(indexType, MVRBTREE_ALGORITHM, new OMVRBTreeIndexEngine<OIdentifiable>(), valueContainerAlgorithm);
    } else if (OClass.INDEX_TYPE.NOTUNIQUE.toString().equals(indexType)) {
      return new OIndexNotUnique(indexType, MVRBTREE_ALGORITHM, new OMVRBTreeIndexEngine<Set<OIdentifiable>>(),
          valueContainerAlgorithm);
    } else if (OClass.INDEX_TYPE.FULLTEXT.toString().equals(indexType)) {
      return new OIndexFullText(indexType, MVRBTREE_ALGORITHM, new OMVRBTreeIndexEngine<Set<OIdentifiable>>(),
          valueContainerAlgorithm);
    } else if (OClass.INDEX_TYPE.DICTIONARY.toString().equals(indexType)) {
      return new OIndexDictionary(indexType, MVRBTREE_ALGORITHM, new OMVRBTreeIndexEngine<OIdentifiable>(), valueContainerAlgorithm);
    }

    throw new OConfigurationException("Unsupported type : " + indexType);
  }

  private OIndexInternal<?> createSBTreeIndex(String indexType, String valueContainerAlgorithm) {
    if (OClass.INDEX_TYPE.UNIQUE.toString().equals(indexType)) {
      return new OIndexUnique(indexType, SBTREE_ALGORITHM, new OSBTreeIndexEngine<OIdentifiable>(), valueContainerAlgorithm);
    } else if (OClass.INDEX_TYPE.NOTUNIQUE.toString().equals(indexType)) {
      return new OIndexNotUnique(indexType, SBTREE_ALGORITHM, new OSBTreeIndexEngine<Set<OIdentifiable>>(), valueContainerAlgorithm);
    } else if (OClass.INDEX_TYPE.FULLTEXT.toString().equals(indexType)) {
      return new OIndexFullText(indexType, SBTREE_ALGORITHM, new OSBTreeIndexEngine<Set<OIdentifiable>>(), valueContainerAlgorithm);
    } else if (OClass.INDEX_TYPE.DICTIONARY.toString().equals(indexType)) {
      return new OIndexDictionary(indexType, SBTREE_ALGORITHM, new OSBTreeIndexEngine<OIdentifiable>(), valueContainerAlgorithm);
    }

    throw new OConfigurationException("Unsupported type : " + indexType);
  }
}
