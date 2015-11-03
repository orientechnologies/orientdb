/*
 * Copyright 2014 Orient Technologies.
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

package com.orientechnologies.lucene;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.lucene.builder.ODocBuilder;
import com.orientechnologies.lucene.builder.OQueryBuilderImpl;
import com.orientechnologies.lucene.engine.OLuceneStorage;
import com.orientechnologies.lucene.engine.OLuceneFullTextExpIndexEngine;
import com.orientechnologies.lucene.engine.OLuceneIndexEngineDelegate;
import com.orientechnologies.lucene.index.OLuceneFullTextExpIndex;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.lucene.index.OLuceneSpatialIndex;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.index.OIndexFactory;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.spatial.shape.OShapeFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import java.util.*;

public class OLuceneIndexFactory implements OIndexFactory, ODatabaseLifecycleListener {

  public static final String LUCENE_ALGORITHM = "LUCENE";
  public static final String LUCENEEXP_ALGORITHM = "LUCENEEXP";

  private static final Set<String> TYPES;
  private static final Set<String> ALGORITHMS;

  static {
    final Set<String> types = new HashSet<String>();
    types.add(OClass.INDEX_TYPE.FULLTEXT.toString());
    types.add(OClass.INDEX_TYPE.SPATIAL.toString());
    types.add(OClass.INDEX_TYPE.FULLTEXTEXP.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  static {
    final Set<String> algorithms = new HashSet<String>();
    algorithms.add(LUCENE_ALGORITHM);
    algorithms.add(LUCENEEXP_ALGORITHM);
    ALGORITHMS = Collections.unmodifiableSet(algorithms);
  }

  private final Map<String, OIndexInternal>                    db2luceneindexes;
  private final Map<ODatabaseDocumentInternal, OLuceneStorage> db2luceneEngine;
  private final OLuceneSpatialManager                          spatialManager;

  public OLuceneIndexFactory() {
    this(false);
  }

  public OLuceneIndexFactory(boolean manual) {
    spatialManager = new OLuceneSpatialManager(OShapeFactory.INSTANCE);
    if (!manual) Orient.instance().addDbLifecycleListener(this);

    db2luceneindexes = new HashMap<String, OIndexInternal>();
    db2luceneEngine = new HashMap<ODatabaseDocumentInternal, OLuceneStorage>();
  }

  @Override
  public int getLastVersion() {
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
  public OIndexInternal<?> createIndex(String name, ODatabaseDocumentInternal database, String indexType, String algorithm,
                                       String valueContainerAlgorithm, ODocument metadata, int version)
      throws OConfigurationException {

    OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) database.getStorage().getUnderlying();

    OBinarySerializer<?> objectSerializer = storage.getComponentsFactory().binarySerializerFactory
        .getObjectSerializer(OLuceneMockSpatialSerializer.INSTANCE.getId());

    if (objectSerializer == null) {
      storage.getComponentsFactory().binarySerializerFactory
          .registerSerializer(OLuceneMockSpatialSerializer.INSTANCE, OType.EMBEDDED);
    }

    if (metadata == null) metadata = new ODocument().field("analyzer", StandardAnalyzer.class.getName());

    if (OClass.INDEX_TYPE.FULLTEXT.toString().equals(indexType)) {
      return new OLuceneFullTextIndex(name, indexType, LUCENE_ALGORITHM, version, storage, valueContainerAlgorithm, metadata);
    } else if (OClass.INDEX_TYPE.SPATIAL.toString().equals(indexType)) {
      return new OLuceneSpatialIndex(name, indexType, LUCENE_ALGORITHM, version, storage, valueContainerAlgorithm, metadata);
    } else if (OClass.INDEX_TYPE.FULLTEXTEXP.toString().equals(indexType)) {

      OLogManager.instance()
                 .info(this, "create index - database:: %s , indexName:: %s , algo:: %s , valuecontalgo:: %s", database.getName(),
                       name, algorithm, valueContainerAlgorithm);
      if (!db2luceneindexes.containsKey(database.getName())) db2luceneindexes.put(name, new OLuceneFullTextExpIndex(name, indexType,
                                                                                                                    LUCENEEXP_ALGORITHM,
                                                                                                                    version,
                                                                                                                    storage,
                                                                                                                    valueContainerAlgorithm,
                                                                                                                    metadata));

      return new OLuceneFullTextExpIndex(name, indexType, LUCENEEXP_ALGORITHM, version, storage, valueContainerAlgorithm, metadata);

    }
    throw new OConfigurationException("Unsupported type : " + algorithm);
  }

  @Override
  public OIndexEngine createIndexEngine(String algorithm, String name, Boolean durableInNonTxMode, OStorage storage, int version,
                                        Map<String, String> engineProperties) {


    if (LUCENEEXP_ALGORITHM.equalsIgnoreCase(algorithm)) {
      final ODatabaseDocumentInternal database = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();

      OLogManager.instance()
                 .info(this, "CREATE ENGINE database:: %s , name:: %s , algoritmh:: %s", database.getName(), name, algorithm);
      if (!db2luceneEngine.containsKey(database)) {
        OLogManager.instance()
                   .info(this, "REGISTERING name:: %s , algoritmh:: %s , engProps:: %s", name, algorithm, engineProperties);

        db2luceneEngine.put(database, new OLuceneStorage(name, new ODocBuilder(), new OQueryBuilderImpl()));

      }
      return new OLuceneFullTextExpIndexEngine(name, db2luceneEngine.get(database), new ODocBuilder(), new OQueryBuilderImpl());
    }
    return new OLuceneIndexEngineDelegate(name, durableInNonTxMode, storage, version);

  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.REGULAR;
  }

  @Override
  public void onCreate(ODatabaseInternal iDatabase) {
    spatialManager.init((ODatabaseDocumentTx) iDatabase);
  }

  @Override
  public void onOpen(ODatabaseInternal iDatabase) {
    spatialManager.init((ODatabaseDocumentTx) iDatabase);
  }

  @Override
  public void onClose(ODatabaseInternal iDatabase) {

  }

  @Override
  public void onDrop(final ODatabaseInternal iDatabase) {
    try {
      OLogManager.instance().debug(this, "Dropping Lucene indexes...");
      for (OIndex idx : iDatabase.getMetadata().getIndexManager().getIndexes()) {
        if (idx.getInternal() instanceof OLuceneIndex) {
          OLogManager.instance().debug(this, "- index '%s'", idx.getName());
          idx.delete();
        }
      }
    } catch (Exception e) {
      OLogManager.instance().warn(this, "Error on dropping Lucene indexes", e);
    }
  }

  @Override
  public void onCreateClass(ODatabaseInternal iDatabase, OClass iClass) {

  }

  @Override
  public void onDropClass(ODatabaseInternal iDatabase, OClass iClass) {

  }

}
