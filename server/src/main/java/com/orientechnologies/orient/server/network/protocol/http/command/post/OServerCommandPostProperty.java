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
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import java.io.IOException;
import java.util.Map;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequestException;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandPostProperty extends OServerCommandAuthenticatedDbAbstract {
	private static final String		PROPERTY_TYPE_JSON_FIELD	= "propertyType";
	private static final String		LINKED_CLASS_JSON_FIELD		= "linkedClass";
	private static final String		LINKED_TYPE_JSON_FIELD		= "linkedType";
	private static final String[]	NAMES											= { "POST|property/*" };

	@Override
	public boolean execute(final OHttpRequest iRequest) throws Exception {
		ODatabaseDocumentTx db = null;
		try {
			db = getProfiledDatabaseInstance(iRequest);
			if (iRequest.content == null || iRequest.content.length() <= 0)
				return addSingleProperty(iRequest, db);
			else {
				return addMultipreProperties(iRequest, db);
			}
		} finally {
			if (db != null)
				OSharedDocumentDatabase.release(db);
		}
	}

	@SuppressWarnings("unused")
	protected boolean addSingleProperty(final OHttpRequest iRequest, final ODatabaseDocumentTx db) throws InterruptedException,
			IOException {
		String[] urlParts = checkSyntax(iRequest.url, 4,
				"Syntax error: property/<database>/<class-name>/<property-name>/[<property-type>]/[<link-type>]");

		iRequest.data.commandInfo = "Create property";
		iRequest.data.commandDetail = urlParts[2] + "." + urlParts[3];

		if (db.getMetadata().getSchema().getClass(urlParts[2]) == null)
			throw new IllegalArgumentException("Invalid class '" + urlParts[2] + "'");

		final OClass cls = db.getMetadata().getSchema().getClass(urlParts[2]);

		final String propertyName = urlParts[3];

		final OType propertyType = urlParts.length > 4 ? OType.valueOf(urlParts[4]) : OType.STRING;

		switch (propertyType) {
		case LINKLIST:
		case LINKMAP:
		case LINKSET: {
			if (urlParts.length < 6) {
				throw new OHttpRequestException("Syntax error: property named " + propertyName + " is declared as " + propertyType
						+ " but linked type is not declared: property/<database>/<class-name>/<property-name>/<property-type>/<link-type>");
			}
			final OType linkType = OType.valueOf(urlParts[5]);
			final OClass linkClass = db.getMetadata().getSchema().getClass(urlParts[5]);
			if (linkType != null && linkClass != null) {
				throw new IllegalArgumentException(
						"linked type declared as "
								+ urlParts[5]
								+ " can be either a Type or a Class, use the JSON document usage instead. See 'http://code.google.com/p/orient/w/edit/OrientDB_REST'");
			} else if (linkType != null) {
				final OProperty prop = cls.createProperty(propertyName, propertyType, linkType);
			} else if (linkClass != null) {
				final OProperty prop = cls.createProperty(propertyName, propertyType, linkClass);
			} else {
				throw new IllegalArgumentException("property named " + propertyName + " is declared as " + propertyType
						+ " but linked type is not declared");
			}
		}
			break;
		case LINK: {
			if (urlParts.length < 6) {
				throw new OHttpRequestException("Syntax error: property named " + propertyName + " is declared as " + propertyType
						+ " but linked type is not declared: property/<database>/<class-name>/<property-name>/<property-type>/<link-type>");
			}
			final String linkClass = urlParts[5];
			if (linkClass != null) {
				final OProperty prop = cls.createProperty(propertyName, propertyType, db.getMetadata().getSchema().getClass(linkClass));
			} else {
				throw new IllegalArgumentException("property named " + propertyName + " is declared as " + propertyType
						+ " but linked Class is not declared");
			}

		}
			break;

		default:
			final OProperty prop = cls.createProperty(propertyName, propertyType);
			break;
		}

		sendTextContent(iRequest, OHttpUtils.STATUS_CREATED_CODE, OHttpUtils.STATUS_CREATED_DESCRIPTION, null,
				OHttpUtils.CONTENT_TEXT_PLAIN, cls.properties().size());

		return false;
	}

	@SuppressWarnings({ "unchecked", "unused" })
	protected boolean addMultipreProperties(final OHttpRequest iRequest, final ODatabaseDocumentTx db) throws InterruptedException,
			IOException {
		String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: property/<database>/<class-name>");

		iRequest.data.commandInfo = "Create property";
		iRequest.data.commandDetail = urlParts[2];

		if (db.getMetadata().getSchema().getClass(urlParts[2]) == null)
			throw new IllegalArgumentException("Invalid class '" + urlParts[2] + "'");

		final OClass cls = db.getMetadata().getSchema().getClass(urlParts[2]);

		final ODocument propertiesDoc = new ODocument().fromJSON(iRequest.content);

		for (String propertyName : propertiesDoc.fieldNames()) {
			final Map<String, String> doc = (Map<String, String>) propertiesDoc.field(propertyName);
			final OType propertyType = OType.valueOf(doc.get(PROPERTY_TYPE_JSON_FIELD));
			switch (propertyType) {
			case LINKLIST:
			case LINKMAP:
			case LINKSET: {
				final String linkType = doc.get(LINKED_TYPE_JSON_FIELD);
				final String linkClass = doc.get(LINKED_CLASS_JSON_FIELD);
				if (linkType != null) {
					final OProperty prop = cls.createProperty(propertyName, propertyType, OType.valueOf(linkType));
				} else if (linkClass != null) {
					final OProperty prop = cls.createProperty(propertyName, propertyType, db.getMetadata().getSchema().getClass(linkClass));
				} else {
					throw new IllegalArgumentException("property named " + propertyName + " is declared as " + propertyType
							+ " but linked type is not declared");
				}
			}
				break;
			case LINK: {
				final String linkClass = doc.get(LINKED_CLASS_JSON_FIELD);
				if (linkClass != null) {
					final OProperty prop = cls.createProperty(propertyName, propertyType, db.getMetadata().getSchema().getClass(linkClass));
				} else {
					throw new IllegalArgumentException("property named " + propertyName + " is declared as " + propertyType
							+ " but linked Class is not declared");
				}

			}
				break;

			default:
				final OProperty prop = cls.createProperty(propertyName, propertyType);
				break;
			}
		}

		sendTextContent(iRequest, OHttpUtils.STATUS_CREATED_CODE, OHttpUtils.STATUS_CREATED_DESCRIPTION, null,
				OHttpUtils.CONTENT_TEXT_PLAIN, cls.properties().size());

		return false;
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}
}
