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
package com.orientechnologies.orient.core.storage;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OIndexEngineCallback;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * This is the gateway interface between the Database side and the storage. Provided implementations are: Local, Remote and Memory.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @see com.orientechnologies.orient.core.storage.impl.memory.ODirectMemoryStorage
 */

public interface OStorageIndex extends OStorage {
  boolean hasIndexRangeQuerySupport(int indexId) throws OInvalidIndexEngineIdException;

  int addIndexEngine(String engineName, String algorithm, String indexType, OIndexDefinition indexDefinition,
      OBinarySerializer valueSerializer, boolean isAutomatic, Boolean durableInNonTxMode, int version,
      Map<String, String> engineProperties, Set<String> clustersToIndex, ODocument metadata);

  void deleteIndexEngine(int indexId) throws OInvalidIndexEngineIdException;

  int loadIndexEngine(String name);

  int loadExternalIndexEngine(String engineName, String algorithm, String indexType, OIndexDefinition indexDefinition,
      OBinarySerializer valueSerializer, boolean isAutomatic, Boolean durableInNonTxMode, int version,
      Map<String, String> engineProperties);

  boolean indexContainsKey(int indexId, Object key) throws OInvalidIndexEngineIdException;

  Object getIndexFirstKey(int indexId) throws OInvalidIndexEngineIdException;

  Object getIndexLastKey(int indexId) throws OInvalidIndexEngineIdException;

  void clearIndex(int indexId) throws OInvalidIndexEngineIdException;

  boolean removeKeyFromIndex(int indexId, Object key) throws OInvalidIndexEngineIdException;

  OIndexKeyCursor getIndexKeyCursor(int indexId) throws OInvalidIndexEngineIdException;

  OIndexEngine getIndexEngine(int indexId) throws OInvalidIndexEngineIdException;

  <T> T callIndexEngine(boolean atomicOperation, boolean readOperation, int indexId, OIndexEngineCallback<T> callback)
      throws OInvalidIndexEngineIdException;

  Object getIndexValue(int indexId, Object key) throws OInvalidIndexEngineIdException;

  OIndexCursor iterateIndexEntriesBetween(int indexId, Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, OIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException;

  OIndexCursor iterateIndexEntriesMajor(int indexId, Object fromKey, boolean isInclusive, boolean ascSortOrder,
      OIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException;

  OIndexCursor iterateIndexEntriesMinor(int indexId, final Object toKey, final boolean isInclusive, boolean ascSortOrder,
      OIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException;

  long getIndexSize(int indexId, OIndexEngine.ValuesTransformer transformer) throws OInvalidIndexEngineIdException;

  OIndexCursor getIndexCursor(int indexId, OIndexEngine.ValuesTransformer valuesTransformer) throws OInvalidIndexEngineIdException;

  OIndexCursor getIndexDescCursor(int indexId, OIndexEngine.ValuesTransformer valuesTransformer)
      throws OInvalidIndexEngineIdException;

  boolean validatedPutIndexValue(int indexId, Object key, OIdentifiable value,
      OIndexEngine.Validator<Object, OIdentifiable> validator) throws OInvalidIndexEngineIdException;

  void putIndexValue(int indexId, Object key, Object value) throws OInvalidIndexEngineIdException;

  void updateIndexEntry(int indexId, Object key, Callable<Object> valueCreator) throws OInvalidIndexEngineIdException;
}
