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
package com.orientechnologies.orient.core.dictionary;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;

@SuppressWarnings("unchecked")
public class ODictionary<T extends Object> {
	private OIndex<OIdentifiable>	index;

	public ODictionary(final OIndex<OIdentifiable> iIndex) {
		index = iIndex;
	}

	public <RET extends T> RET get(final String iKey) {
		final OIdentifiable value = index.get(iKey);
		if (value == null)
			return null;

		return (RET) value.getRecord();
	}

	public <RET extends T> RET get(final String iKey, final String iFetchPlan) {
		final OIdentifiable value = index.get(iKey);
		if (value == null)
			return null;

		if (value instanceof ORID)
			return (RET) ODatabaseRecordThreadLocal.INSTANCE.get().load(((ORID) value), iFetchPlan);

		return (RET) ((ODocument) value).load(iFetchPlan);
	}

	public void put(final String iKey, final Object iValue) {
		index.put(iKey, (OIdentifiable) iValue);
	}

	public boolean containsKey(final String iKey) {
		return index.contains(iKey);
	}

	public boolean remove(final String iKey) {
		return index.remove(iKey);
	}

	public long size() {
		return index.getSize();
	}

	public Iterable<Object> keys() {
		return index.keys();
	}

	public OIndex<OIdentifiable> getIndex() {
		return index;
	}
}
