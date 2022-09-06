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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.index.engine.v1.OCellBTreeIndexEngine;
import com.orientechnologies.orient.core.index.engine.v1.OCellBTreeMultiValueIndexEngine;
import com.orientechnologies.orient.core.index.engine.v1.OCellBTreeSingleValueIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.engine.ORemoteIndexEngine;
import com.orientechnologies.orient.core.storage.index.engine.OSBTreeIndexEngine;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Default OrientDB index factory for indexes based on SBTree.<br>
 * Supports index types:
 *
 * <ul>
 *   <li>UNIQUE
 *   <li>NOTUNIQUE
 *   <li>FULLTEXT
 *   <li>DICTIONARY
 * </ul>
 */
public class ODefaultIndexFactory implements OIndexFactory {

  private static final String SBTREE_ALGORITHM = "SBTREE";
  static final String SBTREE_BONSAI_VALUE_CONTAINER = "SBTREEBONSAISET";
  public static final String NONE_VALUE_CONTAINER = "NONE";
  static final String CELL_BTREE_ALGORITHM = "CELL_BTREE";

  private static final Set<String> TYPES;
  private static final Set<String> ALGORITHMS;

  static {
    final Set<String> types = new HashSet<>();
    types.add(OClass.INDEX_TYPE.UNIQUE.toString());
    types.add(OClass.INDEX_TYPE.NOTUNIQUE.toString());
    types.add(OClass.INDEX_TYPE.FULLTEXT.toString());
    types.add(OClass.INDEX_TYPE.DICTIONARY.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  static {
    final Set<String> algorithms = new HashSet<>();
    algorithms.add(SBTREE_ALGORITHM);
    algorithms.add(CELL_BTREE_ALGORITHM);

    ALGORITHMS = Collections.unmodifiableSet(algorithms);
  }

  static boolean isMultiValueIndex(final String indexType) {
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
   *
   * <ul>
   *   <li>UNIQUE
   *   <li>NOTUNIQUE
   *   <li>FULLTEXT
   *   <li>DICTIONARY
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
    if (valueContainerAlgorithm == null) {
      valueContainerAlgorithm = NONE_VALUE_CONTAINER;
    }

    if (version < 0) {
      version = getLastVersion(algorithm);
    }

    return createSBTreeIndex(
        name,
        indexType,
        valueContainerAlgorithm,
        metadata,
        (OAbstractPaginatedStorage) storage,
        version,
        algorithm);
  }

  private static OIndexInternal createSBTreeIndex(
      String name,
      String indexType,
      String valueContainerAlgorithm,
      ODocument metadata,
      OAbstractPaginatedStorage storage,
      int version,
      String algorithm) {

    final int binaryFormatVersion = storage.getConfiguration().getBinaryFormatVersion();

    if (OClass.INDEX_TYPE.UNIQUE.toString().equals(indexType)) {
      return new OIndexUnique(
          name,
          indexType,
          algorithm,
          version,
          storage,
          valueContainerAlgorithm,
          metadata,
          binaryFormatVersion);
    } else if (OClass.INDEX_TYPE.NOTUNIQUE.toString().equals(indexType)) {
      return new OIndexNotUnique(
          name,
          indexType,
          algorithm,
          version,
          storage,
          valueContainerAlgorithm,
          metadata,
          binaryFormatVersion);
    } else if (OClass.INDEX_TYPE.FULLTEXT.toString().equals(indexType)) {
      OLogManager.instance()
          .warnNoDb(
              ODefaultIndexFactory.class,
              "You are creating native full text index instance. "
                  + "That is unsafe because this type of index is deprecated and will be removed in future.");
      return new OIndexFullText(
          name,
          indexType,
          algorithm,
          version,
          storage,
          valueContainerAlgorithm,
          metadata,
          binaryFormatVersion);
    } else if (OClass.INDEX_TYPE.DICTIONARY.toString().equals(indexType)) {
      return new OIndexDictionary(
          name,
          indexType,
          algorithm,
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
    switch (algorithm) {
      case SBTREE_ALGORITHM:
        return OSBTreeIndexEngine.VERSION;
      case CELL_BTREE_ALGORITHM:
        return OCellBTreeIndexEngine.VERSION;
    }

    throw new IllegalStateException("Invalid algorithm name " + algorithm);
  }

  @Override
  public OBaseIndexEngine createIndexEngine(
      int indexId,
      String algorithm,
      String name,
      OStorage storage,
      int version,
      @SuppressWarnings("SpellCheckingInspection") boolean multiValue) {

    if (algorithm == null) {
      throw new OIndexException("Name of algorithm is not specified");
    }
    final OBaseIndexEngine indexEngine;
    String storageType = storage.getType();

    if (storageType.equals("distributed")) {
      storageType = storage.getType();
    }

    switch (storageType) {
      case "memory":
      case "plocal":
        switch (algorithm) {
          case SBTREE_ALGORITHM:
            indexEngine =
                new OSBTreeIndexEngine(indexId, name, (OAbstractPaginatedStorage) storage, version);
            break;
          case CELL_BTREE_ALGORITHM:
            if (multiValue) {
              indexEngine =
                  new OCellBTreeMultiValueIndexEngine(
                      indexId, name, (OAbstractPaginatedStorage) storage, version);
            } else {
              indexEngine =
                  new OCellBTreeSingleValueIndexEngine(
                      indexId, name, (OAbstractPaginatedStorage) storage, version);
            }
            break;
          default:
            throw new IllegalStateException("Invalid name of algorithm :'" + "'");
        }
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
