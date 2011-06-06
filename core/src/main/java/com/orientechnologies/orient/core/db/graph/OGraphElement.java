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

import java.util.Set;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordListener;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

/**
 * Document wrapper class. Useful when you want to manage POJO but without using the Object Database. The mapping from/to the
 * ODocument instance is in charge to the developer.
 * 
 * @See {@link ODatabaseDocument}
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public abstract class OGraphElement extends ODocumentWrapper implements ORecordListener {
	protected ODatabaseGraphTx	database;

	public OGraphElement(final ODatabaseGraphTx iDatabase, final ORID iRID) {
		this(iDatabase, new ODocument((ODatabaseRecord) iDatabase.getUnderlying(), iRID));
		document.setTrackingChanges(false);
	}

	public OGraphElement(final ODatabaseGraphTx iDatabase, final String iClassName) {
		this(iDatabase, new ODocument((ODatabaseRecord) iDatabase.getUnderlying(), iClassName));
		document.setTrackingChanges(false);
	}

	public OGraphElement(final ODatabaseGraphTx iDatabase, final ODocument iDocument) {
		super(iDocument);
		database = iDatabase;
		document.setTrackingChanges(false);
		init();
	}

	/**
	 * Resets the object to be reused. Used by iterators.
	 */
	public abstract void reset();

	public abstract void delete();

	public ORID getId() {
		return document.getIdentity();
	}

	public <RET extends OGraphElement> RET setLabel(final String iLabel) {
		document.field(OGraphDatabase.LABEL, iLabel);
		return (RET) this;
	}

	public String getLabel() {
		return document.field(OGraphDatabase.LABEL);
	}

	public Object get(final String iPropertyName) {
		return document.field(iPropertyName);
	}

	public <RET extends OGraphElement> RET set(final String iPropertyName, final Object iPropertyValue) {
		document.field(iPropertyName, iPropertyValue, null);
		return (RET) this;
	}

	public Object remove(final String iPropertyName) {
		return document.removeField(iPropertyName);
	}

	public Set<String> propertyNames() {
		return document.fieldNames();
	}

	public OGraphElement setDocument(final ODocument iDocument) {
		document = iDocument;
		return this;
	}

	public ODatabaseGraphTx getDatabase() {
		return database;
	}

	private void init() {
		document.setListener(this);
	}
}
