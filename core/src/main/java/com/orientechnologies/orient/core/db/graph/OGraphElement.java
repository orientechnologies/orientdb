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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

/**
 * Document wrapper class. Useful when you want to manage POJO but without using the Object Database. The mapping from/to the
 * ODocument instance is in charge to the developer.
 * 
 * @See {@link ODatabaseDocument}
 * @author Luca Garulli
 */
public abstract class OGraphElement extends ODocumentWrapper {
	public OGraphElement(final ODatabaseRecord<?> iDatabase, final ORID iRID) {
		this(new ODocument(iDatabase, iRID));
	}

	public OGraphElement(final ODatabaseRecord<?> iDatabase, final String iClassName) {
		this(new ODocument(iDatabase, iClassName));
	}

	public OGraphElement(final ODocument iDocument) {
		super(iDocument);
	}

	public Object get(final String iPropertyName) {
		return document.field(iPropertyName);
	}

	@SuppressWarnings("unchecked")
	public <RET extends OGraphElement> RET set(final String iPropertyName, final Object iPropertyValue) {
		document.field(iPropertyName, iPropertyValue, null);
		return (RET) this;
	}

}
