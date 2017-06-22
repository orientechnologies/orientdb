/*
 * Copyright 2010-2014 OrientDB LTD (info(-at-)orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.metadata.schema.clusterselection;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Returns always the first cluster configured.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ODefaultClusterSelectionStrategy implements OClusterSelectionStrategy {
  public static final String NAME = "default";

  public int getCluster(final OClass iClass, final ODocument doc) {
    return iClass.getDefaultClusterId();
  }

  @Override
  public int getCluster(OClass iClass, int[] selection, ODocument doc) {
    return iClass.getDefaultClusterId();
  }

  @Override
  public String getName() {
    return NAME;
  }
}
