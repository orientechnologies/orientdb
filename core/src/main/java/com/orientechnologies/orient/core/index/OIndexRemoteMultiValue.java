/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.*;
import java.util.Map.Entry;

/**
 * Proxied index.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OIndexRemoteMultiValue extends OIndexRemote<Collection<OIdentifiable>> {
	protected final static String	QUERY_GET	= "select FLATTEN( rid ) from index:%s where key = ?";

	public OIndexRemoteMultiValue(String iName, String iWrappedType, ORID iRid, final OIndexDefinition iIndexDefinition) {
		super(iName, iWrappedType, iRid, iIndexDefinition);
	}

	public Collection<OIdentifiable> get(final Object iKey) {
		final OCommandRequest cmd = formatCommand(QUERY_GET, name);
		return (Collection<OIdentifiable>) getDatabase().command(cmd).execute(iKey);
	}

	public Iterator<Entry<Object, Collection<OIdentifiable>>> iterator() {
		final OCommandRequest cmd = formatCommand(QUERY_ENTRIES, name);
		final Collection<ODocument> result = getDatabase().command(cmd).execute();

		Map<Object, Collection<OIdentifiable>> map = new HashMap<Object, Collection<OIdentifiable>>();
		for (ODocument d : result) {
			Collection<OIdentifiable> rids = map.get(d.field("key"));
			if (rids == null) {
				rids = new HashSet<OIdentifiable>();
				map.put(d.field("key"), rids);
			}

			rids.add((OIdentifiable) d.field("rid"));
		}

		return map.entrySet().iterator();
	}
}
