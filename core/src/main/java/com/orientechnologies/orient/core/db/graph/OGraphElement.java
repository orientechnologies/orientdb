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
package com.orientechnologies.orient.core.db.graph;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * GraphDB Node.
 */
public abstract class OGraphElement {
	protected ODocument	document;

	public OGraphElement(final ODatabaseRecord<?> iDatabase, final ORID iRID) {
		this(new ODocument(iDatabase, iRID));
	}

	public OGraphElement(final ODatabaseRecord<?> iDatabase, final String iClassName) {
		this(new ODocument(iDatabase, iClassName));
	}

	public OGraphElement(final ODocument iDocument) {
		document = iDocument;
	}

	public Object get(final String iPropertyName) {
		return document.field(iPropertyName);
	}

	@SuppressWarnings("unchecked")
	public <RET extends OGraphElement> RET set(final String iPropertyName, final Object iPropertyValue) {
		document.field(iPropertyName, iPropertyValue, null);
		return (RET) this;
	}

	public void reload() {
		document.load();
	}

	public void save() {
		document.save();
	}

	public ODocument getDocument() {
		return document;
	}
}
