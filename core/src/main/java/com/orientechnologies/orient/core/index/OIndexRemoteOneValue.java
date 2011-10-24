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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Proxied single value index.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexRemoteOneValue extends OIndexRemote<OIdentifiable> {
	protected final static String	QUERY_GET	= "select rid from index:%s where key = ?";

	public OIndexRemoteOneValue(final String iName, final String iWrappedType, final ORID iRid,
			final OIndexDefinition iIndexDefinition, final ODocument iConfiguration) {
		super(iName, iWrappedType, iRid, iIndexDefinition, iConfiguration);
	}

	public OIdentifiable get(final Object iKey) {
		final OCommandRequest cmd = formatCommand(QUERY_GET, name);
		final List<OIdentifiable> result = getDatabase().command(cmd).execute(iKey);
		if (result != null && !result.isEmpty())
			return ((OIdentifiable) ((ODocument) result.get(0).getRecord()).field("rid")).getIdentity();
		return null;
	}

	public Iterator<Entry<Object, OIdentifiable>> iterator() {
		final OCommandRequest cmd = formatCommand(QUERY_ENTRIES, name);
		final Collection<ODocument> result = getDatabase().command(cmd).execute();

		final Map<Object, OIdentifiable> map = new HashMap<Object, OIdentifiable>();
		for (final ODocument d : result)
			map.put(d.field("key"), (OIdentifiable) d.field("rid"));

		return map.entrySet().iterator();
	}
}
