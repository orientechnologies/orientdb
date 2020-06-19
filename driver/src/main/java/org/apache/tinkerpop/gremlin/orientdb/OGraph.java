package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.index.OIndex;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

/** Created by Enrico Risa on 31/08/2017. */
public interface OGraph extends Graph {

  @Deprecated
  String labelToClassName(String label, String prefix);

  @Deprecated
  String classNameToLabel(String className);

  String createEdgeClass(final String label);

  String createVertexClass(final String label);

  @Deprecated
  Stream<OrientVertex> getIndexedVertices(OIndex index, Iterator<Object> valueIter);

  @Deprecated
  Stream<OrientEdge> getIndexedEdges(OIndex index, Iterator<Object> valueIter);

  ODatabaseDocument getRawDatabase();

  OGremlinResultSet executeSql(String sql, Map params);

  OGremlinResultSet querySql(String sql, Map params);

  @Deprecated
  Set<String> getIndexedKeys(final Class<? extends Element> elementClass, String label);

  boolean existClass(String label);

  OElementFactory elementFactory();
}
