/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.tinkerpop.blueprints.impls.orient;

import com.tinkerpop.blueprints.util.DefaultVertexQuery;

/**
 * OrientDB implementation for vertex centric queries.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (http://orientdb.com)
 */
public class OrientVertexQuery extends DefaultVertexQuery {

  public OrientVertexQuery(final OrientVertex vertex) {
    super(vertex);
  }

  /**
   * (Blueprints Extension) Counts the total items found. This method is more efficient than
   * executing the query and browse the returning Iterable.
   *
   * @return
   */
  @Override
  public long count() {
    if (hasContainers.isEmpty()) {
      // NO CONDITIONS: USE THE FAST COUNT
      long counter = ((OrientVertex) vertex).countEdges(direction, labels);
      if (limit != Integer.MAX_VALUE && counter > limit) return limit;
      return counter;
    }

    // ITERATE EDGES TO MATCH CONDITIONS
    return super.count();
  }
}
