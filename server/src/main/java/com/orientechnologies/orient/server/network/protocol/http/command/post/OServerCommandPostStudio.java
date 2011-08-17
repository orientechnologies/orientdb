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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

@SuppressWarnings("unchecked")
public class OServerCommandPostStudio extends OServerCommandAuthenticatedDbAbstract {
	private static final String[]	NAMES	= { "POST|studio/*" };

	public boolean execute(final OHttpRequest iRequest) throws Exception {
		ODatabaseDocumentTx db = null;

		try {
			final String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: studio/<database>/<context>");

			db = getProfiledDatabaseInstance(iRequest);

			final String req = iRequest.content;

			// PARSE PARAMETERS
			String operation = null;
			String rid = null;
			String className = null;

			final Map<String, String> fields = new HashMap<String, String>();

			final String[] params = req.split("&");
			String value;

			for (String p : params) {
				String[] pairs = p.split("=");
				value = pairs.length == 1 ? null : pairs[1];

				if ("oper".equals(pairs[0]))
					operation = value;
				else if ("0".equals(pairs[0]))
					rid = value;
				else if ("1".equals(pairs[0]))
					className = value;
				else if (pairs[0].startsWith(ODocumentHelper.ATTRIBUTE_CLASS))
					className = value;
				else if (pairs[0].startsWith("@") || pairs[0].equals("id"))
					continue;
				else {
					fields.put(pairs[0], value);
				}
			}

			String context = urlParts[2];
			if ("document".equals(context))
				executeDocument(iRequest, db, operation, rid, className, fields);
			else if ("classes".equals(context))
				executeClasses(iRequest, db, operation, rid, className, fields);
			else if ("clusters".equals(context))
				executeClusters(iRequest, db, operation, rid, className, fields);
			else if ("classProperties".equals(context))
				executeClassProperties(iRequest, db, operation, rid, className, fields);

		} finally {
			if (db != null)
				OSharedDocumentDatabase.release(db);
		}
		return false;
	}

	private void executeClassProperties(final OHttpRequest iRequest, final ODatabaseDocumentTx db, final String operation,
			final String rid, final String className, final Map<String, String> fields) throws IOException {
		// GET THE TARGET CLASS
		final OClass cls = db.getMetadata().getSchema().getClass(rid);
		if (cls == null) {
			sendTextContent(iRequest, OHttpUtils.STATUS_INTERNALERROR, "Error", null, OHttpUtils.CONTENT_TEXT_PLAIN, "Error: Class '"
					+ rid + "' not found.");
			return;
		}

		if ("add".equals(operation)) {
			iRequest.data.commandInfo = "Studio add property";

			try {
				OType type = OType.valueOf(fields.get("type"));

				OPropertyImpl prop;
				if (type == OType.LINK || type == OType.LINKLIST || type == OType.LINKSET || type == OType.LINKMAP)
					prop = (OPropertyImpl) cls.createProperty(fields.get("name"), type,
							db.getMetadata().getSchema().getClass(fields.get("linkedClass")));
				else
					prop = (OPropertyImpl) cls.createProperty(fields.get("name"), type);

				if (fields.get("linkedType") != null)
					prop.setLinkedType(OType.valueOf(fields.get("linkedType")));
				if (fields.get("mandatory") != null)
					prop.setMandatory("on".equals(fields.get("mandatory")));
				if (fields.get("notNull") != null)
					prop.setNotNull("on".equals(fields.get("notNull")));
				if (fields.get("min") != null)
					prop.setMin(fields.get("min"));
				if (fields.get("max") != null)
					prop.setMax(fields.get("max"));
				if (fields.get("indexed") != null)
					prop.createIndex(fields.get("indexed").equals("Unique") ? OProperty.INDEX_TYPE.UNIQUE : OProperty.INDEX_TYPE.NOTUNIQUE);

				sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN,
						"Property " + fields.get("name") + " created successfully");

			} catch (Exception e) {
				sendTextContent(iRequest, OHttpUtils.STATUS_INTERNALERROR, "Error on creating a new property in class " + rid + ": " + e,
						null, OHttpUtils.CONTENT_TEXT_PLAIN, "Error on creating a new property in class " + rid + ": " + e);
			}
		} else if ("del".equals(operation)) {
			iRequest.data.commandInfo = "Studio delete property";

			cls.dropProperty(className);

			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN,
					"Property " + fields.get("name") + " deleted successfully.");
		}
	}

	private void executeClasses(final OHttpRequest iRequest, final ODatabaseDocumentTx db, final String operation, final String rid,
			final String className, final Map<String, String> fields) throws IOException {
		if ("add".equals(operation)) {
			iRequest.data.commandInfo = "Studio add class";

			// int defCluster = fields.get("defaultCluster") != null ? Integer.parseInt(fields.get("defaultCluster")) : db
			// .getDefaultClusterId();

			try {
				final String superClassName = fields.get("superClass");
				final OClass superClass;
				if (superClassName != null)
					superClass = db.getMetadata().getSchema().getClass(superClassName);
				else
					superClass = null;

				final OClass cls = db.getMetadata().getSchema().createClass(fields.get("name"), superClass);

				final String alias = fields.get("alias");
				if (alias != null)
					cls.setShortName(alias);

				sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, "Class '" + rid
						+ "' created successfully with id=" + db.getMetadata().getSchema().getClasses().size());

			} catch (Exception e) {
				sendTextContent(iRequest, OHttpUtils.STATUS_INTERNALERROR, "Error on creating the new class '" + rid + "': " + e, null,
						OHttpUtils.CONTENT_TEXT_PLAIN, "Error on creating the new class '" + rid + "': " + e);
			}
		} else if ("del".equals(operation)) {
			iRequest.data.commandInfo = "Studio delete class";

			db.getMetadata().getSchema().dropClass(rid);

			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, "Class '" + rid
					+ "' deleted successfully.");
		}
	}

	private void executeClusters(final OHttpRequest iRequest, final ODatabaseDocumentTx db, final String operation, final String rid,
			final String iClusterName, final Map<String, String> fields) throws IOException {
		if ("add".equals(operation)) {
			iRequest.data.commandInfo = "Studio add cluster";

		} else if ("del".equals(operation)) {
			iRequest.data.commandInfo = "Studio delete cluster";
		}
	}

	private void executeDocument(final OHttpRequest iRequest, final ODatabaseDocumentTx db, final String operation, final String rid,
			final String className, final Map<String, String> fields) throws IOException {
		if ("edit".equals(operation)) {
			iRequest.data.commandInfo = "Studio edit document";

			if (rid == null)
				throw new IllegalArgumentException("Record ID not found in request");

			ODocument doc = new ODocument(db, className, new ORecordId(rid));
			doc = (ODocument) doc.load();

			// BIND ALL CHANGED FIELDS
			Object oldValue;
			Object newValue;
			for (Entry<String, String> f : fields.entrySet()) {
				oldValue = doc.field(f.getKey());
				newValue = f.getValue();

				if (oldValue != null) {
					if (oldValue instanceof ORecord<?>) {
						ORecord<?> rec = (ORecord<?>) oldValue;
						String parsedRid = f.getValue();
						if (parsedRid != null && parsedRid.charAt(0) == '#')
							parsedRid = parsedRid.substring(1);

						if (!rec.getIdentity().toString().equals(parsedRid)) {
							// CHANGED RID
							rec.reset();
							((ORecordId) rec.getIdentity()).fromString(parsedRid);

							// RELOAD TO ASSURE IT EXISTS
							rec.load();
						}
						newValue = oldValue;
					} else if (oldValue instanceof Collection<?>) {
						newValue = new ArrayList<ODocument>();

						if (f.getValue() != null) {
							String[] items = f.getValue().split(",");
							for (String s : items) {
								((List<ODocument>) newValue).add(new ODocument(db, new ORecordId(s)));
							}
						}
					}
				}

				doc.field(f.getKey(), newValue);
			}

			doc.save();
			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, "Record " + rid
					+ " updated successfully.");
		} else if ("add".equals(operation)) {
			iRequest.data.commandInfo = "Studio create document";

			final ODocument doc = new ODocument(db, className);

			// BIND ALL CHANGED FIELDS
			for (Entry<String, String> f : fields.entrySet())
				doc.field(f.getKey(), f.getValue());

			doc.save();
			sendTextContent(iRequest, 201, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, "Record " + doc.getIdentity()
					+ " updated successfully.");

		} else if ("del".equals(operation)) {
			iRequest.data.commandInfo = "Studio delete document";

			if (rid == null)
				throw new IllegalArgumentException("Record ID not found in request");

			final ODocument doc = new ODocument(db, new ORecordId(rid));
			doc.load();
			doc.delete();
			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, "Record " + rid
					+ " deleted successfully.");

		} else
			sendTextContent(iRequest, 500, "Error", null, OHttpUtils.CONTENT_TEXT_PLAIN, "Operation not supported");
	}

	public String[] getNames() {
		return NAMES;
	}
}
