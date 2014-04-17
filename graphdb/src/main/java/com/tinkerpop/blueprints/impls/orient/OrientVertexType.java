/*
 * Copyright 2010-2014 Orient Technologies LTD (info--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassAbstractDelegate;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OPropertyAbstractDelegate;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.OStorage;
import com.tinkerpop.blueprints.Direction;

/**
 * Represents a Vertex class.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public class OrientVertexType extends OClassAbstractDelegate {
  public static final String      CLASS_NAME = "V";
  protected final OrientBaseGraph graph;

  public class OrientVertexProperty extends OPropertyAbstractDelegate {
    protected final OrientBaseGraph graph;

    public OrientVertexProperty(final OrientBaseGraph iGraph, final OProperty iProperty) {
      super(iProperty);
      graph = iGraph;
    }

    public boolean getOrdered() {
      final String value = delegate.getCustom("ordered");
      return Boolean.parseBoolean(value);
    }

    public OrientVertexProperty setOrdered(final boolean iOrdered) {
      delegate.setCustom("ordered", Boolean.toString(iOrdered));
      return this;
    }
  }

  public OrientVertexType(final OrientBaseGraph graph, final OClass delegate) {
    super(delegate);
    this.graph = graph;
  }

  protected static final void checkType(final OClass iType) {
    if (iType == null)
      throw new IllegalArgumentException("Vertex class is null");

    if (!iType.isSubClassOf(CLASS_NAME))
      throw new IllegalArgumentException("Type error. The class " + iType + " does not extend class '" + CLASS_NAME
          + "' and therefore cannot be considered a Vertex");
  }

  @Override
  public OrientVertexProperty createProperty(final String iPropertyName, final OType iType) {
    return new OrientVertexProperty(graph, delegate.createProperty(iPropertyName, iType));
  }

  @Override
  public OrientVertexProperty createProperty(final String iPropertyName, final OType iType, final OClass iLinkedClass) {
    return new OrientVertexProperty(graph, delegate.createProperty(iPropertyName, iType, iLinkedClass));
  }

  @Override
  public OrientVertexProperty createProperty(final String iPropertyName, final OType iType, final OType iLinkedType) {
    return new OrientVertexProperty(graph, delegate.createProperty(iPropertyName, iType, iLinkedType));
  }

  public OrientVertexProperty createEdgeProperty(final Direction iDirection, String iEdgeClassName) {
    iEdgeClassName = OrientBaseGraph.encodeClassName(iEdgeClassName);

    final boolean useVertexFieldsForEdgeLabels = graph.isUseVertexFieldsForEdgeLabels();

    final String fieldName = OrientVertex.getConnectionFieldName(iDirection, iEdgeClassName, useVertexFieldsForEdgeLabels);

    return new OrientVertexProperty(graph, delegate.createProperty(fieldName, OType.ANY));
  }

  @Override
  public OrientVertexType getSuperClass() {
    return new OrientVertexType(graph, super.getSuperClass());
  }

  @Override
  public OrientVertexType addCluster(final String iClusterName) {
    delegate.addCluster(iClusterName);
    return this;
  }

  @Override
  public OrientVertexType addCluster(final String iClusterName, final OStorage.CLUSTER_TYPE iClusterType) {
    delegate.addCluster(iClusterName, iClusterType);
    return this;
  }

}
