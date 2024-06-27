/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial;

import static com.orientechnologies.lucene.OLuceneIndexFactory.LUCENE_ALGORITHM;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.IndexEngineData;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexFactory;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.index.OIndexMetadata;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.spatial.engine.OLuceneSpatialIndexEngineDelegator;
import com.orientechnologies.spatial.index.OLuceneSpatialIndex;
import com.orientechnologies.spatial.shape.OShapeFactory;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class OLuceneSpatialIndexFactory implements OIndexFactory, ODatabaseLifecycleListener {
  private static final OLogger logger =
      OLogManager.instance().logger(OLuceneSpatialIndexFactory.class);

  private static final Set<String> TYPES;
  private static final Set<String> ALGORITHMS;

  static {
    final Set<String> types = new HashSet<String>();
    types.add(OClass.INDEX_TYPE.SPATIAL.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  static {
    final Set<String> algorithms = new HashSet<String>();
    algorithms.add(LUCENE_ALGORITHM);
    ALGORITHMS = Collections.unmodifiableSet(algorithms);
  }

  private final OLuceneSpatialManager spatialManager;

  public OLuceneSpatialIndexFactory() {
    this(false);
  }

  public OLuceneSpatialIndexFactory(boolean manual) {
    if (!manual) Orient.instance().addDbLifecycleListener(this);

    spatialManager = new OLuceneSpatialManager(OShapeFactory.INSTANCE);
  }

  @Override
  public int getLastVersion(final String algorithm) {
    return 0;
  }

  @Override
  public Set<String> getTypes() {
    return TYPES;
  }

  @Override
  public Set<String> getAlgorithms() {
    return ALGORITHMS;
  }

  @Override
  public OIndexInternal createIndex(OStorage storage, OIndexMetadata im)
      throws OConfigurationException {
    ODocument metadata = im.getMetadata();
    final String indexType = im.getType();
    final String algorithm = im.getAlgorithm();

    OBinarySerializer<?> objectSerializer =
        storage
            .getComponentsFactory()
            .binarySerializerFactory
            .getObjectSerializer(OLuceneMockSpatialSerializer.INSTANCE.getId());

    if (objectSerializer == null) {
      storage
          .getComponentsFactory()
          .binarySerializerFactory
          .registerSerializer(OLuceneMockSpatialSerializer.INSTANCE, OType.EMBEDDED);
    }

    if (metadata == null) {
      metadata = new ODocument().field("analyzer", StandardAnalyzer.class.getName());
      im.setMetadata(metadata);
    }
    if (OClass.INDEX_TYPE.SPATIAL.toString().equals(indexType)) {
      return new OLuceneSpatialIndex(im, storage);
    }
    throw new OConfigurationException("Unsupported type : " + algorithm);
  }

  @Override
  public OBaseIndexEngine createIndexEngine(OStorage storage, IndexEngineData data) {
    return new OLuceneSpatialIndexEngineDelegator(
        data.getIndexId(), data.getName(), storage, data.getVersion());
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.REGULAR;
  }

  @Override
  public void onCreate(ODatabaseInternal iDatabase) {
    spatialManager.init(iDatabase);
  }

  @Override
  public void onOpen(ODatabaseInternal iDatabase) {}

  @Override
  public void onClose(ODatabaseInternal iDatabase) {}

  @Override
  public void onDrop(final ODatabaseInternal db) {
    try {
      if (db.isClosed()) return;

      logger.debug("Dropping spatial indexes...");
      final ODatabaseDocumentInternal internalDb = (ODatabaseDocumentInternal) db;
      for (OIndex idx : internalDb.getMetadata().getIndexManagerInternal().getIndexes(internalDb)) {

        if (idx.getInternal() instanceof OLuceneSpatialIndex) {
          logger.debug("- index '%s'", idx.getName());
          internalDb.getMetadata().getIndexManager().dropIndex(idx.getName());
        }
      }
    } catch (Exception e) {
      logger.warn("Error on dropping spatial indexes", e);
    }
  }

  @Override
  public void onCreateClass(ODatabaseInternal iDatabase, OClass iClass) {}

  @Override
  public void onDropClass(ODatabaseInternal iDatabase, OClass iClass) {}

  @Override
  public void onLocalNodeConfigurationRequest(ODocument iConfiguration) {}
}
