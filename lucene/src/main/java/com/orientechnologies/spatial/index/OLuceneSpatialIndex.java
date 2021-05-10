/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 * <p>
 * For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial.index;

import com.orientechnologies.lucene.index.OLuceneIndexNotUnique;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.spatial.engine.OLuceneSpatialIndexContainer;
import com.orientechnologies.spatial.shape.OShapeFactory;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.shape.Shape;

public class OLuceneSpatialIndex extends OLuceneIndexNotUnique {

  private final OShapeFactory shapeFactory = OShapeFactory.INSTANCE;

  public OLuceneSpatialIndex(
      String name,
      String typeId,
      String algorithm,
      int version,
      OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm,
      ODocument metadata,
      final int binaryFormatVersion) {
    super(
        name,
        typeId,
        algorithm,
        version,
        storage,
        valueContainerAlgorithm,
        metadata,
        binaryFormatVersion);
  }

  @Override
  public OLuceneIndexNotUnique put(Object key, OIdentifiable value) {
    if (key == null) {
      return this;
    }
    return super.put(key, value);
  }

  @Override
  public Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry> interpretTxKeyChanges(
      final OTransactionIndexChangesPerKey changes) {

    try {
      return storage.callIndexEngine(
          false,
          indexId,
          engine -> {
            if (((OLuceneSpatialIndexContainer) engine).isLegacy()) {
              return OLuceneSpatialIndex.super.interpretTxKeyChanges(changes);
            } else {
              return interpretAsSpatial(changes);
            }
          });
    } catch (OInvalidIndexEngineIdException e) {
      e.printStackTrace();
    }

    return super.interpretTxKeyChanges(changes);
  }

  @Override
  protected Object encodeKey(Object key) {

    if (key instanceof ODocument) {
      Shape shape = shapeFactory.fromDoc((ODocument) key);
      return shapeFactory.toGeometry(shape);
    }
    return key;
  }

  @Override
  protected Object decodeKey(Object key) {

    if (key instanceof Geometry) {
      Geometry geom = (Geometry) key;
      return shapeFactory.toDoc(geom);
    }
    return key;
  }

  private static Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry> interpretAsSpatial(
      OTransactionIndexChangesPerKey item) {
    // 1. Handle common fast paths.

    List<OTransactionIndexChangesPerKey.OTransactionIndexEntry> entries = item.getEntriesAsList();
    Map<OIdentifiable, Integer> counters = new LinkedHashMap<>();

    for (OTransactionIndexChangesPerKey.OTransactionIndexEntry entry : entries) {

      Integer counter = counters.get(entry.getValue());
      if (counter == null) {
        counter = 0;
      }
      switch (entry.getOperation()) {
        case PUT:
          counter++;
          break;
        case REMOVE:
          counter--;
          break;
        case CLEAR:
          break;
      }
      counters.put(entry.getValue(), counter);
    }

    OTransactionIndexChangesPerKey changes = new OTransactionIndexChangesPerKey(item.key);

    for (Map.Entry<OIdentifiable, Integer> entry : counters.entrySet()) {
      OIdentifiable oIdentifiable = entry.getKey();
      switch (entry.getValue()) {
        case 1:
          changes.add(oIdentifiable, OTransactionIndexChanges.OPERATION.PUT);
          break;
        case -1:
          changes.add(oIdentifiable, OTransactionIndexChanges.OPERATION.REMOVE);
          break;
      }
    }
    return changes.getEntriesAsList();
  }
}
