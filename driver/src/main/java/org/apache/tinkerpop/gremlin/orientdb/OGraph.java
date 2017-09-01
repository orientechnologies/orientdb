package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * Created by Enrico Risa on 31/08/2017.
 */
public interface OGraph extends Graph {

  String labelToClassName(String label, String prefix);

  String classNameToLabel(String className);

  String createEdgeClass(final String label);

  String createVertexClass(final String label);

  ODatabaseDocument getRawDatabase();

  OElementFactory elementFactory();

}
