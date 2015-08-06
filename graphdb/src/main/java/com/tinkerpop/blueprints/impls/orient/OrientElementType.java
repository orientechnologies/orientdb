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

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassAbstractDelegate;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Arrays;

/**
 * Represents a generic element class
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public abstract class OrientElementType extends OClassAbstractDelegate {
  protected final OrientBaseGraph graph;

  public OrientElementType(final OrientBaseGraph graph, final OClass delegate) {
    super(delegate);
    this.graph = graph;
  }

  @Override
  public OProperty createProperty(final String iPropertyName, final OType iType, final OClass iLinkedClass) {
    return graph.executeOutsideTx(new OCallable<OProperty, OrientBaseGraph>() {
      @Override
      public OProperty call(final OrientBaseGraph g) {
        return OrientElementType.super.createProperty(iPropertyName, iType, iLinkedClass);
      }
    }, "create ", getTypeName(), " property '", iPropertyName, "' as type '", iType.toString(), "' linked class '",
        iLinkedClass.getName(), "'");
  }

  @Override
  public OProperty createProperty(final String iPropertyName, final OType iType, final OType iLinkedType) {
    return graph.executeOutsideTx(new OCallable<OProperty, OrientBaseGraph>() {
      @Override
      public OProperty call(final OrientBaseGraph g) {
        return OrientElementType.super.createProperty(iPropertyName, iType, iLinkedType);
      }
    }, "create ", getTypeName(), " property '", iPropertyName, "' as type '", iType.toString(), "' linked type '",
        iLinkedType.toString(), "'");
  }

  @Override
  public OProperty createProperty(final String iPropertyName, final OType iType) {
    return graph.executeOutsideTx(new OCallable<OProperty, OrientBaseGraph>() {
      @Override
      public OProperty call(final OrientBaseGraph g) {
        return OrientElementType.super.createProperty(iPropertyName, iType);
      }
    }, "create ", getTypeName(), " property '", iPropertyName, "' as type '", iType.toString(), "'");
  }

  @Override
  public OIndex<?> createIndex(final String iName, final INDEX_TYPE iType, final String... fields) {
    return graph.executeOutsideTx(new OCallable<OIndex<?>, OrientBaseGraph>() {
      @Override
      public OIndex<?> call(final OrientBaseGraph g) {
        return OrientElementType.super.createIndex(iName, iType, fields);
      }
    }, "create index '", iName, "' as type '", iType.toString(), "' on fields: " + Arrays.toString(fields));
  }

  @Override
  public OIndex<?> createIndex(final String iName, final String iType, final String... fields) {
    return graph.executeOutsideTx(new OCallable<OIndex<?>, OrientBaseGraph>() {
      @Override
      public OIndex<?> call(final OrientBaseGraph g) {
        return OrientElementType.super.createIndex(iName, iType, fields);
      }
    }, "create index '", iName, "' as type '", iType, "' on fields: " + Arrays.toString(fields));
  }

  @Override
  public OIndex<?> createIndex(final String iName, final INDEX_TYPE iType, final OProgressListener iProgressListener,
      final String... fields) {
    return graph.executeOutsideTx(new OCallable<OIndex<?>, OrientBaseGraph>() {
      @Override
      public OIndex<?> call(final OrientBaseGraph g) {
        return OrientElementType.super.createIndex(iName, iType, iProgressListener, fields);
      }
    }, "create index '", iName, "' as type '", iType.toString(), "' on fields: " + Arrays.toString(fields));
  }

  @Override
  public OIndex<?> createIndex(final String iName, final String iType, final OProgressListener iProgressListener,
      final ODocument metadata, final String algorithm, final String... fields) {
    return graph.executeOutsideTx(new OCallable<OIndex<?>, OrientBaseGraph>() {
      @Override
      public OIndex<?> call(final OrientBaseGraph g) {
        return OrientElementType.super.createIndex(iName, iType, iProgressListener, metadata, algorithm, fields);
      }
    }, "create index '", iName, "' as type '", iType, "' on fields: " + Arrays.toString(fields));
  }

  @Override
  public OIndex<?> createIndex(final String iName, final String iType, final OProgressListener iProgressListener,
      final ODocument metadata, final String... fields) {
    return graph.executeOutsideTx(new OCallable<OIndex<?>, OrientBaseGraph>() {
      @Override
      public OIndex<?> call(final OrientBaseGraph g) {
        return OrientElementType.super.createIndex(iName, iType, iProgressListener, metadata, fields);
      }
    }, "create index '", iName, "' as type '", iType, "' on fields: " + Arrays.toString(fields));
  }

  protected abstract String getTypeName();
}
