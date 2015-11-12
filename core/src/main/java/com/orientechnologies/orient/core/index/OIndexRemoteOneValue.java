/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Proxied single value index.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexRemoteOneValue extends OIndexRemote<OIdentifiable> {
  protected final static String QUERY_GET = "select rid from index:%s where key = ?";

  public OIndexRemoteOneValue(final String iName, final String iWrappedType, final String algorithm,
      final OIndexDefinition iIndexDefinition, final ODocument iConfiguration, final Set<String> clustersToIndex) {
    super(iName, iWrappedType, algorithm, iIndexDefinition, iConfiguration, clustersToIndex);
  }

  public OIdentifiable get(final Object iKey) {
    final OCommandRequest cmd = formatCommand(QUERY_GET, name);
    final List<OIdentifiable> result = getDatabase().command(cmd).execute(iKey);
    if (result != null && !result.isEmpty())
      return ((OIdentifiable) ((ODocument) result.get(0).getRecord()).field("rid")).getIdentity();
    return null;
    // return (OIdentifiable) ((OStorageProxy) getDatabase().getStorage()).indexGet(name, iKey, null);
  }

  public Iterator<Entry<Object, OIdentifiable>> iterator() {
    final OCommandRequest cmd = formatCommand(QUERY_ENTRIES, name);
    final Collection<ODocument> result = getDatabase().command(cmd).execute();

    final Map<Object, OIdentifiable> map = new LinkedHashMap<Object, OIdentifiable>();
    for (final ODocument d : result) {
      d.setLazyLoad(false);
      map.put(d.field("key"), (OIdentifiable) d.field("rid"));
    }

    return map.entrySet().iterator();
  }

  public Iterator<Entry<Object, OIdentifiable>> inverseIterator() {
    final OCommandRequest cmd = formatCommand(QUERY_ENTRIES, name);
    final List<ODocument> result = getDatabase().command(cmd).execute();

    final Map<Object, OIdentifiable> map = new LinkedHashMap<Object, OIdentifiable>();

    for (ListIterator<ODocument> it = result.listIterator(); it.hasPrevious();) {
      ODocument d = it.previous();
      d.setLazyLoad(false);
      map.put(d.field("key"), (OIdentifiable) d.field("rid"));
    }

    return map.entrySet().iterator();
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

}
