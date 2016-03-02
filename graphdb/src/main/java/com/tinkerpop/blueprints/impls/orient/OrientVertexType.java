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

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OPropertyAbstractDelegate;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Direction;

/**
 * Represents a Vertex class.
 *
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public class OrientVertexType extends OrientElementType {
  // Keeping the name in Immutable class because i cannot do the other way around
  public static final String CLASS_NAME = OImmutableClass.VERTEX_CLASS_NAME;

  public class OrientVertexProperty extends OPropertyAbstractDelegate {
    public static final String ORDERED = "ordered";
    protected final OrientBaseGraph graph;

    public OrientVertexProperty(final OrientBaseGraph iGraph, final OProperty iProperty) {
      super(iProperty);
      graph = iGraph;
    }

    public boolean isOrdered() {
      final String value = delegate.getCustom(ORDERED);
      return Boolean.parseBoolean(value);
    }

    public OrientVertexProperty setOrdered(final boolean iOrdered) {
      delegate.setCustom(ORDERED, Boolean.toString(iOrdered));
      return this;
    }

    @Override public OProperty setCustom(final String iName, final String iValue) {
      graph.executeOutsideTx(new OCallable<OrientVertexProperty, OrientBaseGraph>() {
        @Override
        public OrientVertexProperty call(OrientBaseGraph iArgument) {
          delegate.setCustom(iName, iValue);
          return null;
        }
      });
      return this;

    }

    @Override public OProperty setMin(final String min) {
      graph.executeOutsideTx(new OCallable<OrientVertexProperty, OrientBaseGraph>() {
        @Override
        public OrientVertexProperty call(OrientBaseGraph iArgument) {
          delegate.setMin(min);
          return null;
        }
      });
      return this;
    }

    @Override public OProperty setMax(final String max) {
      graph.executeOutsideTx(new OCallable<OrientVertexProperty, OrientBaseGraph>() {
        @Override
        public OrientVertexProperty call(OrientBaseGraph iArgument) {
          delegate.setMax(max);
          return null;
        }
      });
      return this;
    }

    @Override public OProperty setCollate(final OCollate collate) {
      graph.executeOutsideTx(new OCallable<OrientVertexProperty, OrientBaseGraph>() {
        @Override
        public OrientVertexProperty call(OrientBaseGraph iArgument) {
          delegate.setCollate(collate);
          return null;
        }
      });
      return this;
    }

    @Override public OProperty setCollate(final String iCollateName) {
      graph.executeOutsideTx(new OCallable<OrientVertexProperty, OrientBaseGraph>() {
        @Override
        public OrientVertexProperty call(OrientBaseGraph iArgument) {
          delegate.setCollate(iCollateName);
          return null;
        }
      });
      return this;
    }

    @Override public OProperty setDefaultValue(final String defaultValue) {
      graph.executeOutsideTx(new OCallable<OrientVertexProperty, OrientBaseGraph>() {
        @Override
        public OrientVertexProperty call(OrientBaseGraph iArgument) {
          delegate.setDefaultValue(defaultValue);
          return null;
        }
      });
      return this;
    }

    @Override public OProperty setLinkedClass(final OClass oClass) {
      graph.executeOutsideTx(new OCallable<OrientVertexProperty, OrientBaseGraph>() {
        @Override
        public OrientVertexProperty call(OrientBaseGraph iArgument) {
          delegate.setLinkedClass(oClass);
          return null;
        }
      });
      return this;
    }

    @Override public OProperty setLinkedType(final OType type) {
      graph.executeOutsideTx(new OCallable<OrientVertexProperty, OrientBaseGraph>() {
        @Override
        public OrientVertexProperty call(OrientBaseGraph iArgument) {
          delegate.setLinkedType(type);
          return null;
        }
      });
      return this;
    }

    @Override public OProperty setMandatory(final boolean mandatory) {
      graph.executeOutsideTx(new OCallable<OrientVertexProperty, OrientBaseGraph>() {
        @Override
        public OrientVertexProperty call(OrientBaseGraph iArgument) {
          delegate.setMandatory(mandatory);
          return null;
        }
      });
      return this;
    }

    @Override public OProperty setName(final String iName) {
      graph.executeOutsideTx(new OCallable<OrientVertexProperty, OrientBaseGraph>() {
        @Override
        public OrientVertexProperty call(OrientBaseGraph iArgument) {
          delegate.setName(iName);
          return null;
        }
      });
      return this;
    }

    @Override public OProperty setNotNull(final boolean iNotNull) {
      graph.executeOutsideTx(new OCallable<OrientVertexProperty, OrientBaseGraph>() {
        @Override
        public OrientVertexProperty call(OrientBaseGraph iArgument) {
          delegate.setNotNull(iNotNull);
          return null;
        }
      });
      return this;
    }

    @Override public OProperty setReadonly(final boolean iReadonly) {
      graph.executeOutsideTx(new OCallable<OrientVertexProperty, OrientBaseGraph>() {
        @Override
        public OrientVertexProperty call(OrientBaseGraph iArgument) {
          delegate.setReadonly(iReadonly);
          return null;
        }
      });
      return this;
    }

    @Override public OProperty setRegexp(final String regexp) {
      graph.executeOutsideTx(new OCallable<OrientVertexProperty, OrientBaseGraph>() {
        @Override
        public OrientVertexProperty call(OrientBaseGraph iArgument) {
          delegate.setRegexp(regexp);
          return null;
        }
      });
      return this;
    }

    @Override public OProperty setType(final OType iType) {
      graph.executeOutsideTx(new OCallable<OrientVertexProperty, OrientBaseGraph>() {
        @Override
        public OrientVertexProperty call(OrientBaseGraph iArgument) {
          delegate.setType(iType);
          return null;
        }
      });
      return this;
    }
  }

  public OrientVertexType(final OrientBaseGraph graph, final OClass delegate) {
    super(graph, delegate);
  }

  protected static void checkType(final OClass iType) {
    if (iType == null)
      throw new IllegalArgumentException("Vertex class is null");

    if (((iType instanceof OImmutableClass) && !((OImmutableClass) iType).isVertexType()) || !iType.isSubClassOf(CLASS_NAME))
      throw new IllegalArgumentException("Type error. The class '" + iType + "' does not extend class '" + CLASS_NAME
          + "' and therefore cannot be considered a Vertex");
  }

  public OrientVertexProperty createEdgeProperty(final Direction iDirection, String iEdgeClassName) {
    return createEdgeProperty(iDirection, iEdgeClassName, OType.ANY);
  }

  public OrientVertexProperty createEdgeProperty(final Direction iDirection, final String iEdgeClassName, final OType iType) {
    return graph.executeOutsideTx(new OCallable<OrientVertexProperty, OrientBaseGraph>() {
      @Override
      public OrientVertexProperty call(OrientBaseGraph iArgument) {
        final String clsName = OrientBaseGraph.encodeClassName(iEdgeClassName);

        final boolean useVertexFieldsForEdgeLabels = graph.isUseVertexFieldsForEdgeLabels();

        final String fieldName = OrientVertex.getConnectionFieldName(iDirection, clsName, useVertexFieldsForEdgeLabels);

        return new OrientVertexProperty(graph, delegate.createProperty(fieldName, iType));
      }
    });
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
  public OrientVertexProperty createProperty(final String iPropertyName, final OType iType, final OClass iLinkedClass) {
    return new OrientVertexProperty(graph, super.createProperty(iPropertyName, iType, iLinkedClass));
  }

  @Override
  public OrientVertexProperty createProperty(final String iPropertyName, final OType iType, final OType iLinkedType) {
    return new OrientVertexProperty(graph, super.createProperty(iPropertyName, iType, iLinkedType));
  }

  @Override
  public OrientVertexProperty createProperty(final String iPropertyName, final OType iType) {
    return new OrientVertexProperty(graph, super.createProperty(iPropertyName, iType));
  }

  @Override
  protected String getTypeName() {
    return "vertex";
  }
}
