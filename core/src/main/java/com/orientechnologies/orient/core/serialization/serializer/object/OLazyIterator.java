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
package com.orientechnologies.orient.core.serialization.serializer.object;

import java.util.Iterator;

import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.record.impl.ODocument;

@SuppressWarnings({ "unchecked" })
public class OLazyIterator<TYPE> implements Iterator<TYPE> {
	final private ODatabaseObjectTx		database;
	final private Iterator<ODocument>	underlying;

	public OLazyIterator(final ODatabaseObjectTx database, final Iterator<ODocument> iIterator) {
		this.database = database;
		this.underlying = iIterator;
	}

	public TYPE next() {
		final Object value = underlying.next();

		if (value == null)
			return null;

		if (value instanceof ODocument)
			return (TYPE) database.getUserObjectByRecord((ODocument) value);

		return (TYPE) value;
	}

	public boolean hasNext() {
		return underlying.hasNext();
	}

	public void remove() {
		underlying.remove();
	}
}
