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

import com.orientechnologies.orient.core.metadata.schema.OClass;

import java.util.Optional;

/**
 * @author Luigi Dell'Aquila
 */
public interface OEdge extends OElement {
  public static final String DIRECITON_OUT = "out";
  public static final String DIRECITON_IN  = "in";

  public OVertex getFrom();

  public OVertex getTo();

  public void delete();

  default OVertex getVertex(ODirection dir) {
    if (dir == ODirection.IN) {
      return getTo();
    } else if (dir == ODirection.OUT) {
      return getFrom();
    }
    return null;
  }

  //TODO is it needed???
  default boolean isLabeled(String[] labels) {
    if (labels == null) {
      return false;
    }
    String type = "E";
    Optional<OClass> typeClass = getSchemaType();
    if (typeClass.isPresent()) {
      type = typeClass.get().getName();
    }
    for (String s : labels) {
      if (type.equalsIgnoreCase(s)) {
        return true;
      }
    }
    return false;
  }
}
