/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial.index;

import com.orientechnologies.lucene.index.OLuceneIndexNotUnique;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OIndexEngineCallback;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.spatial.engine.OLuceneSpatialIndexContainer;
import com.orientechnologies.spatial.shape.OShapeFactory;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.shape.Shape;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OLuceneSpatialIndex extends OLuceneIndexNotUnique {

  OShapeFactory shapeFactory = OShapeFactory.INSTANCE;

  public OLuceneSpatialIndex(String name, String typeId, String algorithm, int version, OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm, ODocument metadata, final int binaryFormatVersion) {
    super(name, typeId, algorithm, version, storage, valueContainerAlgorithm, metadata, binaryFormatVersion);

  }

  @Override
  public OLuceneIndexNotUnique put(Object key, OIdentifiable singleValue) {
    if (key == null) {
      return this;
    }
    return super.put(key, singleValue);
  }

  @Override
  protected Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry> interpretTxKeyChanges(
      final OTransactionIndexChangesPerKey changes) {

    try {
      return storage.callIndexEngine(false, false, indexId,
          new OIndexEngineCallback<Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry>>() {
            @Override
            public Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry> callEngine(OBaseIndexEngine engine) {
              if (((OLuceneSpatialIndexContainer) engine).isLegacy()) {
                return OLuceneSpatialIndex.super.interpretTxKeyChanges(changes);
              } else {
                return interpretAsSpatial(changes.entries);
              }
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

  protected Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry> interpretAsSpatial(
      List<OTransactionIndexChangesPerKey.OTransactionIndexEntry> entries) {
    // 1. Handle common fast paths.

    List<OTransactionIndexChangesPerKey.OTransactionIndexEntry> newChanges = new ArrayList<OTransactionIndexChangesPerKey.OTransactionIndexEntry>();

    Map<OIdentifiable, Integer> counters = new LinkedHashMap<OIdentifiable, Integer>();

    for (OTransactionIndexChangesPerKey.OTransactionIndexEntry entry : entries) {

      Integer counter = counters.get(entry.value);
      if (counter == null) {
        counter = 0;
      }
      switch (entry.operation) {
      case PUT:
        counter++;
        break;
      case REMOVE:
        counter--;
        break;
      case CLEAR:
        break;
      }
      counters.put(entry.value, counter);
    }

    Set<OIdentifiable> oIdentifiables = counters.keySet();
    for (OIdentifiable oIdentifiable : oIdentifiables) {
      switch (counters.get(oIdentifiable)) {
      case 1:

        newChanges
            .add(new OTransactionIndexChangesPerKey.OTransactionIndexEntry(oIdentifiable, OTransactionIndexChanges.OPERATION.PUT));
        break;
      case -1:
        newChanges.add(
            new OTransactionIndexChangesPerKey.OTransactionIndexEntry(oIdentifiable, OTransactionIndexChanges.OPERATION.REMOVE));
        break;
      }

    }
    return newChanges;
  }
}
