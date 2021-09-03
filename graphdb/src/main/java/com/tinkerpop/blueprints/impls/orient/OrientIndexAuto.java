package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import java.util.ArrayList;
import java.util.List;

public class OrientIndexAuto<T extends OrientElement> implements OrientIndex<T> {

  private static final String ELEMENT_FIELD = "element";
  private static final String VERTEX = "Vertex";
  private static final String EDGE = "Edge";

  private static final String CONFIG_CLASS_PREFIX = "___42_record_map_config_auto_42____";
  private static final String INDEX_SUFFIX = "_index";
  private static final String REVERT_INDEX_SUFFIX = "_revert_index";
  private static final String CLASS_PREFIX = "___42_record_map_auto_42___";
  private static final String KEY_FIELD = "key";
  private static final String VALUE_FIELD = "value";
  private static final String CLASS_NAME_FIELD = "className";

  private final String indexName;
  private final Class<? extends Element> indexClass;
  private final OrientBaseGraph graph;

  public static <T extends OrientElement> OrientIndexAuto<T> create(
      final OrientBaseGraph graph, final String indexName, final Class<T> indexClass) {
    final OSchema schema = graph.getRawGraph().getMetadata().getSchema();
    final String indexClassName = generateClassName(indexName);
    if (schema.getClass(indexClassName) != null) {
      throw ExceptionFactory.indexAlreadyExists(indexName);
    }

    final OClass cls = schema.createClass(indexClassName);
    final OClass configCls = schema.createClass(generateConfigClassName(indexName));

    cls.createProperty(KEY_FIELD, OType.STRING);
    cls.createProperty(VALUE_FIELD, OType.STRING);
    cls.createProperty(ELEMENT_FIELD, OType.LINK);

    cls.createIndex(
        generateIndexName(indexClassName), OClass.INDEX_TYPE.NOTUNIQUE, KEY_FIELD, VALUE_FIELD);
    cls.createIndex(
        generateRevertIndexName(indexClassName), OClass.INDEX_TYPE.NOTUNIQUE, ELEMENT_FIELD);

    final String className;
    if (Vertex.class.isAssignableFrom(indexClass)) className = VERTEX;
    else if (Edge.class.isAssignableFrom(indexClass)) className = EDGE;
    else className = indexClass.getName();

    final ODocument document = new ODocument(configCls);
    document.field(CLASS_NAME_FIELD, className);
    document.save();

    return new OrientIndexAuto<>(graph, indexName, indexClass);
  }

  public static <T extends OrientElement> OrientIndexAuto<T> load(
      final OrientBaseGraph graph, final String indexName, final Class<T> indexClass) {
    final String indexClassName = generateClassName(indexName);

    final OSchema schema = graph.getRawGraph().getMetadata().getSchema();
    if (schema.getClass(indexClassName) == null) {
      return null;
    }

    final String className;
    try (final OResultSet resultSet =
        graph
            .getRawGraph()
            .query("select " + CLASS_NAME_FIELD + " from " + generateConfigClassName(indexName))) {
      if (!resultSet.hasNext()) {
        throw new IllegalStateException(
            "Index " + indexName + " is broken can not find configuration");
      }

      className = resultSet.next().getProperty(CLASS_NAME_FIELD);
    }

    final Class<? extends Element> loadedClass;
    if (VERTEX.equals(className)) {
      loadedClass = Vertex.class;
    } else if (EDGE.equals(className)) {
      loadedClass = Edge.class;
    } else {
      try {
        //noinspection unchecked
        loadedClass = (Class<T>) Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(
            "Index class '"
                + className
                + "' is not registered. Supported ones: Vertex, Edge and custom class that extends them",
            e);
      }
    }

    if (indexClass != null && !indexClass.isAssignableFrom(loadedClass)) {
      throw ExceptionFactory.indexDoesNotSupportClass(indexName, indexClass);
    }

    return new OrientIndexAuto<>(graph, indexName, loadedClass);
  }

  public static void drop(final OrientBaseGraph graph, final String indexName) {
    final OSchema schema = graph.getRawGraph().getMetadata().getSchema();
    final String indexClassName = generateClassName(indexName);

    if (schema.getClass(indexClassName) != null) {
      schema.dropClass(indexClassName);
    }

    final String configClassName = generateConfigClassName(indexName);
    if (schema.getClass(configClassName) != null) {
      schema.dropClass(configClassName);
    }
  }

  static boolean isIndexClass(final String className) {
    return className.contains(CLASS_PREFIX);
  }

  private static String generateConfigClassName(String indexName) {
    return CONFIG_CLASS_PREFIX + indexName;
  }

  private static String generateIndexName(String indexClassName) {
    return indexClassName + INDEX_SUFFIX;
  }

  private static String generateRevertIndexName(String indexClassName) {
    return indexClassName + REVERT_INDEX_SUFFIX;
  }

  private static String generateClassName(final String indexName) {
    return CLASS_PREFIX + indexName;
  }

  static String extractIndexName(String indexClassName) {
    return indexClassName.substring(CLASS_PREFIX.length());
  }

  private OrientIndexAuto(
      OrientBaseGraph graph, String indexName, Class<? extends Element> indexClass) {
    this.graph = graph;
    this.indexName = indexName;
    this.indexClass = indexClass;
  }

  @Override
  public String getIndexName() {
    return indexName;
  }

  @Override
  public Class<T> getIndexClass() {
    //noinspection unchecked
    return (Class<T>) indexClass;
  }

  @Override
  public void put(String key, Object value, T element) {
    final String indexClassName = generateClassName(indexName);

    graph.setCurrentGraphInThreadLocal();
    graph.autoStartTransaction();

    final ODocument document = new ODocument(indexClassName);
    document.field(KEY_FIELD, key);
    document.field(VALUE_FIELD, value.toString());

    final ODocument doc = element.getRecord();
    if (!doc.getIdentity().isValid()) {
      doc.save();
    }

    document.field(ELEMENT_FIELD, doc.getIdentity());
    document.save();
  }

  @Override
  public CloseableIterable<T> get(String key, Object value) {
    graph.setCurrentGraphInThreadLocal();

    final String indexClassName = generateClassName(indexName);
    final List<T> result = new ArrayList<>();
    try (final OResultSet resultSet =
        graph
            .getRawGraph()
            .query(
                "select "
                    + ELEMENT_FIELD
                    + " from "
                    + indexClassName
                    + " where "
                    + KEY_FIELD
                    + " = ? and "
                    + VALUE_FIELD
                    + " = ? ",
                key,
                value.toString())) {
      while (resultSet.hasNext()) {
        final OElement element = resultSet.next().getElementProperty(ELEMENT_FIELD);

        if (element.isVertex()) {
          result.add((T) new OrientVertex(graph, element));
        } else if (element.isEdge()) {
          result.add((T) new OrientEdge(graph, element));
        } else {
          throw new IllegalStateException(
              "Fetched record is not part of graph type system, its type is "
                  + element.getSchemaType());
        }
      }
    }

    return new OrientElementIterable<>(graph, result);
  }

  @Override
  public CloseableIterable<T> query(String key, Object query) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long count(String key, Object value) {
    graph.setCurrentGraphInThreadLocal();

    final String indexClassName = generateClassName(indexName);

    try (final OResultSet resultSet =
        graph
            .getRawGraph()
            .query(
                "select count(*) as count from "
                    + indexClassName
                    + " where "
                    + KEY_FIELD
                    + " = ? and "
                    + VALUE_FIELD
                    + " = ? ",
                key,
                value.toString())) {
      if (resultSet.hasNext()) {
        return resultSet.next().getProperty("count");
      }
    }

    return 0;
  }

  @Override
  public void remove(String key, Object value, T element) {
    graph.setCurrentGraphInThreadLocal();
    graph.autoStartTransaction();

    final String indexClassName = generateClassName(indexName);
    graph
        .getRawGraph()
        .command(
            "delete from "
                + indexClassName
                + " where "
                + KEY_FIELD
                + " = ? and "
                + VALUE_FIELD
                + " = ? and "
                + ELEMENT_FIELD
                + " = ?",
            key,
            value.toString(),
            element.getIdentity())
        .close();
  }

  @Override
  public void removeElement(T element) {
    graph.setCurrentGraphInThreadLocal();
    graph.autoStartTransaction();

    final String indexClassName = generateClassName(indexName);
    try (final OResultSet resultSet =
        graph
            .getRawGraph()
            .query(
                "select * from " + indexClassName + " where " + ELEMENT_FIELD + " = ?",
                element.getIdentity())) {
      while (resultSet.hasNext()) {
        resultSet.next().getRecord().ifPresent(ORecord::delete);
      }
    }
  }

  public String toString() {
    return StringFactory.indexString(this);
  }
}
