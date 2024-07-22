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

package com.orientechnologies.lucene;

import static com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE.FULLTEXT;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.engine.OLuceneCrossClassIndexEngine;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class OLuceneCrossClassIndexFactory implements OIndexFactory, ODatabaseLifecycleListener {

  public static final String LUCENE_CROSS_CLASS = "LUCENE_CROSS_CLASS";

  private static final Set<String> TYPES;
  private static final Set<String> ALGORITHMS;

  static {
    final Set<String> types = new HashSet<String>();
    types.add(FULLTEXT.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  static {
    final Set<String> algorithms = new HashSet<String>();
    algorithms.add(LUCENE_CROSS_CLASS);
    ALGORITHMS = Collections.unmodifiableSet(algorithms);
  }

  public OLuceneCrossClassIndexFactory() {
    this(false);
  }

  public OLuceneCrossClassIndexFactory(boolean manual) {
    if (!manual) Orient.instance().addDbLifecycleListener(this);
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
  public OIndexInternal createIndex(
      String name,
      OStorage storage,
      String indexType,
      String algorithm,
      String valueContainerAlgorithm,
      ODocument metadata,
      int version)
      throws OConfigurationException {

    OAbstractPaginatedStorage paginated = (OAbstractPaginatedStorage) storage.getUnderlying();

    if (metadata == null) {
      metadata = new ODocument().field("analyzer", StandardAnalyzer.class.getName());
    }

    if (FULLTEXT.toString().equalsIgnoreCase(indexType)) {
      final int binaryFormatVersion = paginated.getConfiguration().getBinaryFormatVersion();
      OLuceneFullTextIndex index =
          new OLuceneFullTextIndex(
              name,
              indexType,
              algorithm,
              version,
              paginated,
              valueContainerAlgorithm,
              metadata,
              binaryFormatVersion);

      return index;
    }

    throw new OConfigurationException("Unsupported type : " + algorithm);
  }

  public OBaseIndexEngine createIndexEngine(
      int indexId,
      String algorithm,
      String indexName,
      Boolean durableInNonTxMode,
      OStorage storage,
      int version,
      int apiVersion,
      boolean multiValue,
      Map<String, String> engineProperties) {

    if (LUCENE_CROSS_CLASS.equalsIgnoreCase(algorithm)) {
      return new OLuceneCrossClassIndexEngine(indexId, storage, indexName);
    }
    throw new OConfigurationException("Unsupported type : " + algorithm);
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.REGULAR;
  }

  @Override
  public void onCreate(ODatabaseInternal db) {
    createCrossClassSearchIndex(db);
  }

  @Override
  public void onOpen(ODatabaseInternal db) {
    createCrossClassSearchIndex(db);
  }

  @Override
  public void onClose(ODatabaseInternal db) {
    OLogManager.instance().debug(this, "onClose");
  }

  @Override
  public void onDrop(final ODatabaseInternal db) {
    try {
      if (db.isClosed()) return;

      OLogManager.instance().debug(this, "Dropping Lucene indexes...");

      final ODatabaseDocumentInternal internal = (ODatabaseDocumentInternal) db;
      internal.getMetadata().getIndexManagerInternal().getIndexes(internal).stream()
          .filter(idx -> idx.getInternal() instanceof OLuceneCrossClassIndexEngine)
          .peek(idx -> OLogManager.instance().debug(this, "deleting index " + idx.getName()))
          .forEach(idx -> idx.delete());

    } catch (Exception e) {
      OLogManager.instance().warn(this, "Error on dropping Lucene indexes", e);
    }
  }

  @Override
  public void onLocalNodeConfigurationRequest(ODocument iConfiguration) {}

  private void createCrossClassSearchIndex(ODatabaseInternal db) {
    final ODatabaseDocumentInternal internal = (ODatabaseDocumentInternal) db;
    final OIndexManagerAbstract indexManager = internal.getMetadata().getIndexManagerInternal();

    if (!indexManager.existsIndex("CrossClassSearchIndex")) {
      OLogManager.instance().info(this, "creating cross class Lucene index");

      db.command("CREATE INDEX CrossClassSearchIndex FULLTEXT ENGINE LUCENE_CROSS_CLASS").close();
    }
  }
}
