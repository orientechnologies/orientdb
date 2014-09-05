/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *  
 */

package com.tinkerpop.blueprints.impls.orient.asynch;

import com.orientechnologies.common.exception.OException;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.concurrent.Future;

public class OrientVertexFuture extends OrientElementFuture<OrientVertex> implements Vertex {
  public OrientVertexFuture(final Future<OrientVertex> future) {
    super(future);
  }

  @Override
  public Iterable<Edge> getEdges(final Direction direction, final String... labels) {
    try {
      return future.get().getEdges(direction, labels);
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  @Override
  public Iterable<Vertex> getVertices(final Direction direction, final String... labels) {
    try {
      return future.get().getVertices(direction, labels);
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  @Override
  public VertexQuery query() {
    try {
      return future.get().query();
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  @Override
  public Edge addEdge(final String label, final Vertex inVertex) {
    try {
      return future.get().addEdge(label, inVertex);
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

}
