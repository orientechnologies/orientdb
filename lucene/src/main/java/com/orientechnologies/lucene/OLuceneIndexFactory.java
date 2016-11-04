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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.engine.OLuceneIndexEngineDelegator;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.index.OIndexFactory;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

public class OLuceneIndexFactory implements OIndexFactory, ODatabaseLifecycleListener {

  public static final String LUCENE_ALGORITHM     = "LUCENE";
  public static final String LUCENE_ALL_ALGORITHM = "LUCENE_ALL";

  private static final Set<String> TYPES;
  private static final Set<String> ALGORITHMS;

  static {
    final Set<String> types = new HashSet<String>();
    types.add(FULLTEXT.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  static {
    final Set<String> algorithms = new HashSet<String>();
    algorithms.add(LUCENE_ALGORITHM);
    algorithms.add(LUCENE_ALL_ALGORITHM);
    ALGORITHMS = Collections.unmodifiableSet(algorithms);
  }

  public OLuceneIndexFactory() {
    this(false);
  }

  public OLuceneIndexFactory(boolean manual) {
    if (!manual)
      Orient.instance().addDbLifecycleListener(this);

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
  public OIndexInternal<?> createIndex(String name, OStorage storage, String indexType, String algorithm,
      String valueContainerAlgorithm, ODocument metadata, int version) throws OConfigurationException {

    OAbstractPaginatedStorage pagStorage = (OAbstractPaginatedStorage) storage.getUnderlying();

    if (metadata == null)
      metadata = new ODocument().field("analyzer", StandardAnalyzer.class.getName());

    if (FULLTEXT.toString().equalsIgnoreCase(indexType)) {

      OLuceneFullTextIndex index = new OLuceneFullTextIndex(name, indexType, algorithm, version, pagStorage,
          valueContainerAlgorithm, metadata);

      return index;
    }
    throw new OConfigurationException("Unsupported type : " + algorithm);
  }

  @Override
  public OIndexEngine createIndexEngine(String algorithm, String name, Boolean durableInNonTxMode, OStorage storage, int version,
      Map<String, String> engineProperties) {

    return new OLuceneIndexEngineDelegator(name,algorithm, durableInNonTxMode, storage, version);

  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.REGULAR;
  }

  @Override
  public void onCreate(ODatabaseInternal iDatabase) {
    OLogManager.instance().debug(this, "onCreate");

  }

  @Override
  public void onOpen(ODatabaseInternal iDatabase) {
    OLogManager.instance().debug(this, "onOpen");

  }

  @Override
  public void onClose(ODatabaseInternal iDatabase) {
    OLogManager.instance().debug(this, "onClose");
  }

  @Override
  public void onDrop(final ODatabaseInternal database) {
    try {
      if (database.isClosed())
        return;

      OLogManager.instance().debug(this, "Dropping Lucene indexes...");

      database.getMetadata().getIndexManager().getIndexes().stream()
          .filter(idx -> idx.getInternal() instanceof OLuceneFullTextIndex)
          .forEach(idx -> {
            OLogManager.instance().debug(this, "- index '%s'", idx.getName());
            idx.delete();
          });

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

  @Override
  public void onLocalNodeConfigurationRequest(ODocument iConfiguration) {
  }
}
