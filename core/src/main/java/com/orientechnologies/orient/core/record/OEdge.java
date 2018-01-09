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

/**
 * @author Luigi Dell'Aquila
 */
public interface OEdge extends OElement {
  public static final String DIRECITON_OUT = "out";
  public static final String DIRECITON_IN  = "in";

  public OVertex getFrom();

  /**
   * returns vertices with specific types
   * @author Thomas Young (YJJThomasYoung@hotmail.com)
   * @param labels names of vertex classes
   * @return
   */
  OVertex getFrom(String... labels);

  public OVertex getTo();

  /**
   * returns vertices with specific types
   * @author Thomas Young (YJJThomasYoung@hotmail.com)
   * @param labels names of vertex classes
   * @return
   */
  OVertex getTo(String... labels);

  public boolean isLightweight();

  default OVertex getVertex(ODirection dir) {
    return getVertex(dir, (String) null);
  }

  /**
   * get vertex with specific types
   * @author Thomas Young (YJJThomasYoung@hotmail.com)
   * @param dir direction
   * @param labels names of vertex classes
   * @return
   */
  default OVertex getVertex(ODirection dir, String... labels) {
    if (dir == ODirection.IN) {
      return getTo(labels);
    } else if (dir == ODirection.OUT) {
      return getFrom(labels);
    }
    return null;
  }
}
