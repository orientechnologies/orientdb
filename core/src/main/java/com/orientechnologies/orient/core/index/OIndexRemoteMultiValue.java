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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Proxied index.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings("unchecked")
public class OIndexRemoteMultiValue extends OIndexRemote<Collection<OIdentifiable>> {
  protected final static String QUERY_GET = "select expand( rid ) from index:%s where key = ?";

  public OIndexRemoteMultiValue(final String iName, final String iWrappedType, final String algorithm, final ORID iRid,
      final OIndexDefinition iIndexDefinition, final ODocument iConfiguration, final Set<String> clustersToIndex, String database) {
    super(iName, iWrappedType, algorithm, iRid, iIndexDefinition, iConfiguration, clustersToIndex, database);
  }

  public Collection<OIdentifiable> get(final Object iKey) {
    try (final OResultSet result = getDatabase().command(String.format(QUERY_GET, name), iKey)) {
      return result.stream().map((res) -> res.getIdentity().orElse(null)).collect(Collectors.toSet());
    }
  }

  public Iterator<Entry<Object, Collection<OIdentifiable>>> iterator() {
    try (final OResultSet result = getDatabase().command(String.format(QUERY_ENTRIES, name))) {

      final Map<Object, Collection<OIdentifiable>> map = result.stream()
          .collect(Collectors.toMap((r) -> r.getProperty("key"), (r) -> r.getProperty("rid")));
      return map.entrySet().iterator();
    }
  }

  public Iterator<OIdentifiable> valuesIterator() {
    throw new UnsupportedOperationException();
  }

  public Iterator<OIdentifiable> valuesInverseIterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }
}
