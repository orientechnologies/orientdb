package com.tinkerpop.blueprints.impls.orient;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexTxAwareMultiValue;
import com.orientechnologies.orient.core.index.OIndexTxAwareOneValue;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StringFactory;
import com.tinkerpop.blueprints.util.WrappingCloseableIterable;

/**
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
@SuppressWarnings("unchecked")
public class OrientIndex<T extends OrientElement> implements Index<T> {
  protected static final String      VERTEX                 = "Vertex";
  protected static final String      EDGE                   = "Edge";
  protected static final String      CONFIG_CLASSNAME       = "blueprintsIndexClass";
  public static final String         CONFIG_RECORD_MAP_NAME = "record_map_name";

  protected static final String      SEPARATOR              = "!=!";

  protected OrientBaseGraph          graph;
  protected OIndex<?>                underlying;
  protected OIndex<?>                recordKeyValueIndex;

  protected Class<? extends Element> indexClass;

  protected OrientIndex(final OrientBaseGraph graph, final String indexName, final Class<? extends Element> indexClass,
      final OType iType) {
    this.graph = graph;
    this.indexClass = indexClass;
    create(indexName, this.indexClass, iType);
  }

  protected OrientIndex(final OrientBaseGraph orientGraph, final OIndex<?> rawIndex) {
    this.graph = orientGraph;
    this.underlying = rawIndex instanceof OIndexTxAwareMultiValue ? rawIndex : new OIndexTxAwareMultiValue(
        orientGraph.getRawGraph(), (OIndex<Collection<OIdentifiable>>) rawIndex);
    load(rawIndex.getMetadata());
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
    if (!doc.getIdentity().isValid())
      doc.save();

    graph.autoStartTransaction();
    underlying.put(keyTemp, doc);
    recordKeyValueIndex.put(new OCompositeKey(element.getIdentity(), keyTemp), element.getIdentity());
  }

  @SuppressWarnings("rawtypes")
  public CloseableIterable<T> get(final String key, final Object iValue) {
    final String keyTemp = key + SEPARATOR + iValue;
    Collection<OIdentifiable> records = (Collection<OIdentifiable>) underlying.get(keyTemp);

    if (records == null || records.isEmpty())
      return new WrappingCloseableIterable(Collections.emptySet());

    return new OrientElementIterable<T>(graph, records);
  }

  public CloseableIterable<T> query(final String key, final Object query) throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }

  public long count(final String key, final Object value) {
    final String keyTemp = key + SEPARATOR + value;
    final Collection<OIdentifiable> records = (Collection<OIdentifiable>) underlying.get(keyTemp);
    return records.size();
  }

  public void remove(final String key, final Object value, final T element) {
    final String keyTemp = key + SEPARATOR + value;
    graph.autoStartTransaction();
    try {
      underlying.remove(keyTemp, element.getRecord());
      recordKeyValueIndex.remove(new OCompositeKey(element.getIdentity(), keyTemp), element.getIdentity());
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public String toString() {
    return StringFactory.indexString(this);
  }

  protected void removeElement(final T element) {
    graph.autoStartTransaction();
    Collection<ODocument> entries = recordKeyValueIndex.getEntriesBetween(new OCompositeKey(element.getIdentity()),
        new OCompositeKey(element.getIdentity()));
    for (ODocument entry : entries) {
      OCompositeKey key = entry.field("key");
      List<Object> keys = key.getKeys();
      underlying.remove(keys.get(1).toString(), element.getIdentity());
      recordKeyValueIndex.remove(key, element.getIdentity());
    }
  }

  public OIndex<?> getUnderlying() {
    return underlying;
  }

  private void create(final String indexName, final Class<? extends Element> indexClass, OType iKeyType) {
    this.indexClass = indexClass;

    if (iKeyType == null)
      iKeyType = OType.STRING;

    this.recordKeyValueIndex = new OIndexTxAwareOneValue(graph.getRawGraph(), (OIndex<OIdentifiable>) graph
        .getRawGraph()
        .getMetadata()
        .getIndexManager()
        .createIndex("__@recordmap@___" + indexName, OClass.INDEX_TYPE.DICTIONARY.toString(),
            new OSimpleKeyIndexDefinition(OType.LINK, OType.STRING), null, null, null));

    final String className;
    if (Vertex.class.isAssignableFrom(indexClass))
      className = VERTEX;
    else if (Edge.class.isAssignableFrom(indexClass))
      className = EDGE;
    else
      className = indexClass.getName();

    final ODocument metadata = new ODocument();
    metadata.field(CONFIG_CLASSNAME, className);
    metadata.field(CONFIG_RECORD_MAP_NAME, recordKeyValueIndex.getName());

    // CREATE THE MAP
    this.underlying = new OIndexTxAwareMultiValue(graph.getRawGraph(), (OIndex<Collection<OIdentifiable>>) graph
        .getRawGraph()
        .getMetadata()
        .getIndexManager()
        .createIndex(indexName, OClass.INDEX_TYPE.NOTUNIQUE.toString(), new OSimpleKeyIndexDefinition(iKeyType), null, null,
            metadata));

  }

  private void load(final ODocument metadata) {
    // LOAD TREEMAP
    final String indexClassName = metadata.field(CONFIG_CLASSNAME);
    final String recordKeyValueMap = metadata.field(CONFIG_RECORD_MAP_NAME);

    if (VERTEX.equals(indexClassName))
      this.indexClass = OrientVertex.class;
    else if (EDGE.equals(indexClassName))
      this.indexClass = OrientEdge.class;
    else
      try {
        this.indexClass = (Class<T>) Class.forName(indexClassName);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Index class '" + indexClassName
            + "' is not registered. Supported ones: Vertex, Edge and custom class that extends them");
      }

    recordKeyValueIndex = new OIndexTxAwareOneValue(graph.getRawGraph(), (OIndex<OIdentifiable>) graph.getRawGraph().getMetadata()
        .getIndexManager().getIndex(recordKeyValueMap));
  }

  public void close() {
    if (underlying != null) {
      underlying.flush();
      underlying = null;
    }
    graph = null;
  }

}
