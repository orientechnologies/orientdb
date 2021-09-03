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
package com.orientechnologies.orient.core.query;

import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemFieldMultiAbstract;
import java.util.List;

/**
 * Represent multiple values in query.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OQueryRuntimeValueMulti {
  protected final OSQLFilterItemFieldMultiAbstract definition;
  protected final List<OCollate> collates;
  protected final Object[] values;

  public OQueryRuntimeValueMulti(
      final OSQLFilterItemFieldMultiAbstract iDefinition,
      final Object[] iValues,
      final List<OCollate> iCollates) {
    definition = iDefinition;
    values = iValues;
    collates = iCollates;
  }

  @Override
  public String toString() {
    if (getValues() == null) return "";

    StringBuilder buffer = new StringBuilder(128);
    buffer.append("[");
    int i = 0;
    for (Object v : getValues()) {
      if (i++ > 0) buffer.append(",");
      buffer.append(v);
    }
    buffer.append("]");
    return buffer.toString();
  }

  public OSQLFilterItemFieldMultiAbstract getDefinition() {
    return definition;
  }

  public OCollate getCollate(final int iIndex) {
    return collates.get(iIndex);
  }

  public Object[] getValues() {
    return values;
  }
}
