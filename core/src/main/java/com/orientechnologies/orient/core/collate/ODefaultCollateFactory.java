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
package com.orientechnologies.orient.core.collate;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Factory to hold collating strategies to compare values in SQL statement and indexes.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ODefaultCollateFactory implements OCollateFactory {

  private static final Map<String, OCollate> COLLATES = new HashMap<String, OCollate>(2);

  static {
    register(new ODefaultCollate());
    register(new OCaseInsensitiveCollate());
  }

  /** @return Set of supported collate names of this factory */
  @Override
  public Set<String> getNames() {
    return COLLATES.keySet();
  }

  /**
   * Returns the requested collate
   *
   * @param name
   */
  @Override
  public OCollate getCollate(final String name) {
    return COLLATES.get(name);
  }

  private static void register(final OCollate iCollate) {
    COLLATES.put(iCollate.getName(), iCollate);
  }
}
