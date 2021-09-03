/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.record;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;

/** @author Luigi Dell'Aquila */
public interface OVertex extends OElement {

  Iterable<OEdge> getEdges(ODirection direction);

  Iterable<OEdge> getEdges(ODirection direction, String... type);

  Iterable<OEdge> getEdges(ODirection direction, OClass... type);

  Iterable<OVertex> getVertices(ODirection direction);

  Iterable<OVertex> getVertices(ODirection direction, String... type);

  Iterable<OVertex> getVertices(ODirection direction, OClass... type);

  OEdge addEdge(OVertex to);

  OEdge addEdge(OVertex to, String type);

  OEdge addEdge(OVertex to, OClass type);

  ORID moveTo(final String iClassName, final String iClusterName);
}
