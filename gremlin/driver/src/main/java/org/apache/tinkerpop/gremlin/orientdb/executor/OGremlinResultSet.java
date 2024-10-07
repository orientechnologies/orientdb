/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.orientdb.executor;

import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Iterator;
import java.util.stream.Stream;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;

/** Created by Enrico Risa on 05/06/2017. */
public class OGremlinResultSet implements Iterable<OGremlinResult>, AutoCloseable {

  private OrientGraph graph;
  private OResultSet inner;

  public OGremlinResultSet(OrientGraph graph, OResultSet inner) {
    this.graph = graph;
    this.inner = inner;
  }

  @Override
  public void close() {
    inner.close();
  }

  @Override
  public Iterator<OGremlinResult> iterator() {
    return new Iterator<OGremlinResult>() {
      @Override
      public boolean hasNext() {
        return inner.hasNext();
      }

      @Override
      public OGremlinResult next() {
        OResult next = inner.next();
        return new OGremlinResult(graph, next);
      }
    };
  }

  public Stream<OGremlinResult> stream() {
    return inner.stream().map((result) -> new OGremlinResult(graph, result));
  }

  public OResultSet getRawResultSet() {
    return inner;
  }
}
