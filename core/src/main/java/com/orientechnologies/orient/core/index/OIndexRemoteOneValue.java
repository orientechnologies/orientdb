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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Proxied single value index.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OIndexRemoteOneValue extends OIndexRemote<OIdentifiable> {
  protected final static String QUERY_GET = "select rid from index:%s where key = ?";

  public OIndexRemoteOneValue(final String iName, final String iWrappedType, final String algorithm, final ORID iRid,
      final OIndexDefinition iIndexDefinition, final ODocument iConfiguration, final Set<String> clustersToIndex, String database) {
    super(iName, iWrappedType, algorithm, iRid, iIndexDefinition, iConfiguration, clustersToIndex, database);
  }

  public OIdentifiable get(final Object iKey) {
    try (final OResultSet result = getDatabase().command(String.format(QUERY_GET, name), iKey)) {
      if (result != null && result.hasNext())
        return ((OIdentifiable) result.next().getProperty("rid"));
      return null;
    }
  }

  public Iterator<Entry<Object, OIdentifiable>> iterator() {
    try (final OResultSet result = getDatabase().command(String.format(QUERY_ENTRIES, name))) {

      final Map<Object, OIdentifiable> map = result.stream()
          .collect(Collectors.toMap((res) -> res.getProperty("key"), (res) -> res.getProperty("rid")));

      return map.entrySet().iterator();
    }
  }

  @Override
  public boolean isUnique() {
    return true;
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

}
