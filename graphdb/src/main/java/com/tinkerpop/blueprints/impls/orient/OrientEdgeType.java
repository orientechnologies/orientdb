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
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;

/**
 * Represents an Edge class.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public class OrientEdgeType extends OrientElementType {
  // Keeping the name in Immutable class because i cannot do the other way around
  public static final String CLASS_NAME = OImmutableClass.EDGE_CLASS_NAME;

  public OrientEdgeType(final OrientBaseGraph graph, final OClass delegate) {
    super(graph, delegate);
  }

  public OrientEdgeType(final OrientBaseGraph graph) {
    super(graph, graph.getRawGraph().getMetadata().getSchema().getClass(CLASS_NAME));
  }

  protected static void checkType(final OClass iType) {
    if (iType == null)
      throw new IllegalArgumentException("Edge class is null");

    if (((iType instanceof OImmutableClass) && !((OImmutableClass) iType).isEdgeType()) || !iType.isSubClassOf(CLASS_NAME))
      throw new IllegalArgumentException("Type error. The class " + iType + " does not extend class '" + CLASS_NAME
          + "' and therefore cannot be considered an Edge");
  }

  @Override
  public OrientEdgeType getSuperClass() {
    return new OrientEdgeType(graph, super.getSuperClass());
  }

  @Override
  public OrientEdgeType addCluster(final String iClusterName) {
    delegate.addCluster(iClusterName);
    return this;
  }

  @Override
  protected String getTypeName() {
    return "edge";
  }
}
