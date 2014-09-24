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
import com.tinkerpop.blueprints.impls.orient.OrientEdge;

import java.util.concurrent.Future;

public class OrientEdgeFuture extends OrientElementFuture<OrientEdge> implements Edge {
  public OrientEdgeFuture(final Future<OrientEdge> future) {
    super(future);
  }

  @Override
  public Vertex getVertex(final Direction direction) throws IllegalArgumentException {
    try {
      return future.get().getVertex(direction);
    } catch (Exception e) {
      throw new OException("Cannot retrieve getVertex()", e);
    }
  }

  @Override
  public String getLabel() {
    try {
      return future.get().getLabel();
    } catch (Exception e) {
      throw new OException("Cannot retrieve label()", e);
    }
  }
}
