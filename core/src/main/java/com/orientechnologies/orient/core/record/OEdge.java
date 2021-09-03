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
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/** @author Luigi Dell'Aquila */
public interface OEdge extends OElement {
  public static final String DIRECTION_OUT = "out";
  public static final String DIRECTION_IN = "in";

  // typo!
  @Deprecated public static final String DIRECITON_OUT = DIRECTION_OUT;
  @Deprecated public static final String DIRECITON_IN = DIRECTION_IN;

  public OVertex getFrom();

  public OVertex getTo();

  public boolean isLightweight();

  default OVertex getVertex(ODirection dir) {
    if (dir == ODirection.IN) {
      return getTo();
    } else if (dir == ODirection.OUT) {
      return getFrom();
    }
    return null;
  }

  default boolean isLabeled(String[] labels) {
    if (labels == null) {
      return true;
    }
    if (labels.length == 0) {
      return true;
    }
    Set<String> types = new HashSet<>();

    Optional<OClass> typeClass = getSchemaType();
    if (typeClass.isPresent()) {
      types.add(typeClass.get().getName());
      typeClass.get().getAllSuperClasses().stream()
          .map(x -> x.getName())
          .forEach(name -> types.add(name));
    } else {
      types.add("E");
    }
    for (String s : labels) {
      for (String type : types) {
        if (type.equalsIgnoreCase(s)) {
          return true;
        }
      }
    }

    return false;
  }
}
