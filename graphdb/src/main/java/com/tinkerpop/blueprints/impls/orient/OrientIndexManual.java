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

package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StringFactory;
import com.tinkerpop.blueprints.util.WrappingCloseableIterable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** @author Luca Garulli (l.garulli--(at)--orientdb.com) (http://orientdb.com) */
@SuppressWarnings("unchecked")
public class OrientIndexManual<T extends OrientElement> implements OrientIndex<T> {
  public static final String CONFIG_CLASSNAME = "blueprintsIndexClass";
  public static final String CONFIG_RECORD_MAP_NAME = "record_map_name";
  protected static final String VERTEX = "Vertex";
  protected static final String EDGE = "Edge";
  protected static final String SEPARATOR = "!=!";

  protected OrientBaseGraph graph;
  protected OIndex underlying;
  protected OIndex recordKeyValueIndex;

  protected Class<? extends Element> indexClass;

  protected OrientIndexManual(
      final OrientBaseGraph graph,
      final String indexName,
      final Class<? extends Element> indexClass,
      final OType iType) {
    this.graph = graph;
    this.indexClass = indexClass;
    create(indexName, this.indexClass, iType);
  }

  protected OrientIndexManual(final OrientBaseGraph orientGraph, final OIndex rawIndex) {
    this.graph = orientGraph;
    this.underlying = rawIndex;

    final ODocument metadata = rawIndex.getMetadata();
    if (metadata == null) {
      load(rawIndex.getConfiguration());
    } else load(metadata);
  }

  public String getIndexName() {
    return underlying.getName();
  }

  public Class<T> getIndexClass() {
    return (Class<T>) this.indexClass;
  }

  public void put(final String key, final Object value, final T element) {
    final String keyTemp = key + SEPARATOR + value;

    final ODocument doc = element.getRecord();
    if (!doc.getIdentity().isValid()) doc.save();

    graph.setCurrentGraphInThreadLocal();
    graph.autoStartTransaction();
    underlying.put(keyTemp, doc);
    recordKeyValueIndex.put(new OCompositeKey(doc.getIdentity(), keyTemp), doc.getIdentity());
  }

  @SuppressWarnings("rawtypes")
  public CloseableIterable<T> get(final String key, final Object iValue) {
    final String keyTemp = key + SEPARATOR + iValue;
    Collection<OIdentifiable> records;
    try (Stream<ORID> rids = underlying.getInternal().getRids(keyTemp)) {
      records = rids.collect(Collectors.toList());
    }

    if (records == null || records.isEmpty())
      return new WrappingCloseableIterable(Collections.emptySet());

    return new OrientElementIterable<T>(graph, records);
  }

  public CloseableIterable<T> query(final String key, final Object query)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }

  public long count(final String key, final Object value) {
    final String keyTemp = key + SEPARATOR + value;
    try (Stream<ORID> rids = underlying.getInternal().getRids(keyTemp)) {
      return rids.count();
    }
  }

  public void remove(final String key, final Object value, final T element) {
    final String keyTemp = key + SEPARATOR + value;
    graph.setCurrentGraphInThreadLocal();
    graph.autoStartTransaction();
    try {
      underlying.remove(keyTemp, element.getRecord());
      recordKeyValueIndex.remove(
          new OCompositeKey(element.getIdentity(), keyTemp), element.getIdentity());
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public String toString() {
    return StringFactory.indexString(this);
  }

  public OIndex getUnderlying() {
    return underlying;
  }

  public void close() {
    if (underlying != null) {
      underlying = null;
    }
    graph = null;
  }

  @Override
  public void removeElement(final T element) {
    graph.setCurrentGraphInThreadLocal();
    graph.autoStartTransaction();

    final OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>(
            "select from index:"
                + recordKeyValueIndex.getName()
                + " where key between ["
                + element.getIdentity()
                + "] and ["
                + element.getIdentity()
                + "]");

    final Collection<ODocument> entries = (Collection<ODocument>) graph.getRawGraph().query(query);

    for (ODocument entry : entries) {
      final OCompositeKey key = entry.field("key");
      final List<Object> keys = key.getKeys();
      underlying.remove(keys.get(1).toString(), element.getIdentity());
      recordKeyValueIndex.remove(key, element.getIdentity());
    }
  }

  private void create(
      final String indexName, final Class<? extends Element> indexClass, OType iKeyType) {
    this.indexClass = indexClass;

    if (iKeyType == null) iKeyType = OType.STRING;

    final ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) graph.getRawGraph();

    this.recordKeyValueIndex =
        db.getMetadata()
            .getIndexManagerInternal()
            .createIndex(
                db,
                "__@recordmap@___" + indexName,
                OClass.INDEX_TYPE.DICTIONARY.toString(),
                new OSimpleKeyIndexDefinition(OType.LINK, OType.STRING),
                null,
                null,
                null);

    final String className;
    if (Vertex.class.isAssignableFrom(indexClass)) className = VERTEX;
    else if (Edge.class.isAssignableFrom(indexClass)) className = EDGE;
    else className = indexClass.getName();

    final ODocument metadata = new ODocument();
    metadata.field(CONFIG_CLASSNAME, className);
    metadata.field(CONFIG_RECORD_MAP_NAME, recordKeyValueIndex.getName());

    // CREATE THE MAP
    if (!db.isRemote()) {
      this.underlying =
          db.getMetadata()
              .getIndexManagerInternal()
              .createIndex(
                  db,
                  indexName,
                  OClass.INDEX_TYPE.NOTUNIQUE.toString(),
                  new OSimpleKeyIndexDefinition(iKeyType),
                  null,
                  null,
                  metadata);
    } else {
      this.underlying =
          db.getMetadata()
              .getIndexManagerInternal()
              .createIndex(
                  db,
                  indexName,
                  OClass.INDEX_TYPE.NOTUNIQUE.toString(),
                  new OSimpleKeyIndexDefinition(iKeyType),
                  null,
                  null,
                  metadata);
    }
  }

  private void load(final ODocument metadata) {
    // LOAD TREEMAP
    final String indexClassName = metadata.field(CONFIG_CLASSNAME);
    final String recordKeyValueMap = metadata.field(CONFIG_RECORD_MAP_NAME);

    if (VERTEX.equals(indexClassName)) this.indexClass = Vertex.class;
    else if (EDGE.equals(indexClassName)) this.indexClass = Edge.class;
    else
      try {
        this.indexClass = (Class<T>) Class.forName(indexClassName);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(
            "Index class '"
                + indexClassName
                + "' is not registered. Supported ones: Vertex, Edge and custom class that extends them",
            e);
      }

    final ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) graph.getRawGraph();
    if (recordKeyValueMap == null) recordKeyValueIndex = buildKeyValueIndex(metadata);
    else {
      final OIndex index =
          database.getMetadata().getIndexManagerInternal().getIndex(database, recordKeyValueMap);
      recordKeyValueIndex = index;
    }
  }

  private OIndex buildKeyValueIndex(final ODocument metadata) {
    final ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) graph.getRawGraph();
    final OIndex index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .createIndex(
                database,
                "__@recordmap@___" + underlying.getName(),
                OClass.INDEX_TYPE.DICTIONARY.toString(),
                new OSimpleKeyIndexDefinition(OType.LINK, OType.STRING),
                null,
                null,
                null);
    final OIndex recordKeyValueIndex = index;

    final List<ODocument> entries =
        graph
            .getRawGraph()
            .query(new OSQLSynchQuery<Object>("select  from index:" + underlying.getName()));

    for (ODocument entry : entries) {
      final OIdentifiable rid = entry.field("rid");
      if (rid != null) recordKeyValueIndex.put(new OCompositeKey(rid, entry.field("key")), rid);
    }

    metadata.field(CONFIG_RECORD_MAP_NAME, recordKeyValueIndex.getName());
    return recordKeyValueIndex;
  }
}
