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
package com.orientechnologies.orient.server.network.protocol.http;

import java.io.IOException;
import java.io.StringWriter;
import java.net.Socket;
import java.net.URLDecoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.text.OChannelTextServer;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolException;

public class ONetworkProtocolHttpDb extends ONetworkProtocolHttpAbstract {
	private DateFormat	dateTimeFormat	= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	@Override
	public void config(Socket iSocket, OClientConnection iConnection) throws IOException {
		setName("HTTP-DB");
		super.config(iSocket, iConnection);
	}

	@Override
	public void doGet(final String iURI, final String iRequest, final OChannelTextServer iChannel) throws ONetworkProtocolException {
		if (iURI == null || iURI.length() == 0)
			return;

		if (OHttpUtils.URL_SEPARATOR.equals(iURI) || iURI.startsWith("/www")) {
			directAccess(iURI);
			return;
		}

		// GET THE OPERATION
		final String[] parts = OHttpUtils.getParts(iURI);
		if (parts.length == 0)
			return;

		try {
			if (parts[0].equals("document"))
				getDocument(parts);
			else if (parts[0].equals("query"))
				getQuery(parts);
			else if (parts[0].equals("dictionary"))
				getDictionary(parts);
			else if (parts[0].equals("cluster"))
				getCluster(parts);
			else if (parts[0].equals("database"))
				getDatabase(parts);
			else if (parts[0].equals("class"))
				getClass(parts);
			else if (parts[0].equals("server"))
				getServer(parts);
			else
				throw new IllegalArgumentException("Operation '" + parts[0] + "' not supported");

		} catch (Exception e) {
			handleError(e);
		}
	}

	@Override
	public void doPost(final String iURI, final String iRequest, final OChannelTextServer iChannel) throws ONetworkProtocolException {
		if (iURI == null || iURI.length() == 0)
			return;

		// GET THE OPERATION
		final String[] parts = OHttpUtils.getParts(iURI);
		if (parts.length == 0)
			return;

		try {
			if (parts[0].equals("studio-document"))
				postDocument(parts, iRequest);
			else
				throw new IllegalArgumentException("Operation '" + parts[0] + "' not supported");

		} catch (Exception e) {
			handleError(e);
		}
	}

	@Override
	public void doPut(final String iURI, final String iRequest, final OChannelTextServer iChannel) throws ONetworkProtocolException {
	}

	@Override
	public void doDelete(final String iURI, final String iRequest, final OChannelTextServer iChannel)
			throws ONetworkProtocolException {
	}

	private void getDocument(final String[] iParts) throws Exception {
		ODatabaseDocumentTx db = null;

		commandType = OChannelBinaryProtocol.RECORD_LOAD;

		try {
			checkSyntax(iParts, 3, "Syntax error: document/<database>/<record-id>");

			db = OSharedDocumentDatabase.acquireDatabase(iParts[1]);

			final ORecord<?> rec = db.load(new ORecordId(iParts[2]));
			sendRecordContent(rec);
		} finally {
			if (db != null)
				OSharedDocumentDatabase.releaseDatabase(db);
		}
	}

	@SuppressWarnings("unchecked")
	private void postDocument(final String[] iParts, final String iRequest) throws Exception {
		ODatabaseDocumentTx db = null;

		try {
			checkSyntax(iParts, 2, "Syntax error: document/<database>");
			db = OSharedDocumentDatabase.acquireDatabase(iParts[1]);

			String req = URLDecoder.decode(iRequest, "UTF-8");

			// PARSE PARAMETERS
			String operation = null;
			String rid = null;
			String className = null;

			Map<String, String> fields = new HashMap<String, String>();

			String[] params = req.split("&");
			String key;
			String value;

			for (String p : params) {
				String[] pairs = p.split("=");
				key = pairs[0];
				value = pairs.length == 1 ? null : pairs[1];

				if ("oper".equals(pairs[0]))
					operation = value;
				else if ("0".equals(pairs[0]))
					rid = value;
				else if ("1".equals(pairs[0]))
					className = value;
				else if (pairs[0].startsWith("_") || pairs[0].equals("id"))
					continue;
				else {
					fields.put(pairs[0], value);
				}
			}

			if ("edit".equals(operation)) {
				commandType = OChannelBinaryProtocol.RECORD_UPDATE;

				if (rid == null)
					throw new IllegalArgumentException("Record ID not found in request");

				ODocument doc = new ODocument(db, className, new ORecordId(rid));
				doc.load();

				// BIND ALL CHANGED FIELDS
				Object oldValue;
				Object newValue;
				for (Entry<String, String> f : fields.entrySet()) {
					oldValue = doc.field(f.getKey());
					newValue = f.getValue();

					if (oldValue != null) {
						if (oldValue instanceof ORecord<?>)
							newValue = new ORecordId(f.getValue());
						else if (oldValue instanceof Collection<?>) {
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
				sendTextContent(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Record " + rid + " updated successfully.");
			} else if ("add".equals(operation)) {
				commandType = OChannelBinaryProtocol.RECORD_CREATE;

				ODocument doc = new ODocument(db, className);

				// BIND ALL CHANGED FIELDS
				for (Entry<String, String> f : fields.entrySet())
					doc.field(f.getKey(), f.getValue());

				doc.save();
				sendTextContent(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Record " + doc.getIdentity()
						+ " updated successfully.");

			} else if ("del".equals(operation)) {
				commandType = OChannelBinaryProtocol.RECORD_DELETE;

				if (rid == null)
					throw new IllegalArgumentException("Record ID not found in request");

				ODocument doc = new ODocument(db, new ORecordId(rid));
				doc.delete();
				sendTextContent(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Record " + rid + " deleted successfully.");

			} else
				sendTextContent(500, "Error", OHttpUtils.CONTENT_TEXT_PLAIN, "Operation not supported");

		} finally {
			if (db != null)
				OSharedDocumentDatabase.releaseDatabase(db);
		}
	}

	private void getDictionary(final String[] iParts) throws Exception {
		commandType = OChannelBinaryProtocol.DICTIONARY_LOOKUP;

		checkSyntax(iParts, 3, "Syntax error: dictionary/<database>/<key>");

		ODatabaseDocumentTx db = null;

		try {
			db = OSharedDocumentDatabase.acquireDatabase(iParts[1]);

			final ORecord<?> record = db.getDictionary().get(iParts[2]);
			if (record == null)
				throw new ORecordNotFoundException("Key '" + iParts[2] + "' was not found in the database dictionary");

			sendRecordContent(record);

		} finally {
			if (db != null)
				OSharedDocumentDatabase.releaseDatabase(db);
		}
	}

	@SuppressWarnings("unchecked")
	private void getQuery(final String[] iParts) throws Exception {
		commandType = OChannelBinaryProtocol.COMMAND;

		checkSyntax(
				iParts,
				3,
				"Syntax error: query/sql/<query-text>[/<limit>]<br/>Limit is optional and is setted to 20 by default. Set expressely to 0 to have no limits.");

		ODatabaseDocumentTx db = null;

		try {
			db = OSharedDocumentDatabase.acquireDatabase(iParts[1]);

			final int limit = iParts.length > 3 ? Integer.parseInt(iParts[3]) : 20;
			final String text = URLDecoder.decode(iParts[2].trim(), "UTF-8");
			if (!text.toLowerCase().startsWith("select"))
				throw new IllegalArgumentException("Only SQL Select are valid using HTTP GET");

			final List<ORecord<?>> response = (List<ORecord<?>>) db.command(new OSQLSynchQuery<ORecordSchemaAware<?>>(text, limit))
					.execute();

			sendRecordsContent(response);

		} finally {
			if (db != null)
				OSharedDocumentDatabase.releaseDatabase(db);
		}
	}

	private void getCluster(final String[] iParts) throws Exception {
		commandType = OChannelBinaryProtocol.COMMAND;

		checkSyntax(
				iParts,
				3,
				"Syntax error: cluster/<database>/<cluster-name>[/<limit>]<br/>Limit is optional and is setted to 20 by default. Set expressely to 0 to have no limits.");

		ODatabaseDocumentTx db = null;

		try {
			db = OSharedDocumentDatabase.acquireDatabase(iParts[1]);

			if (db.getClusterIdByName(iParts[2]) == -1)
				throw new IllegalArgumentException("Invalid cluster '" + iParts[2] + "'");

			final int limit = iParts.length > 3 ? Integer.parseInt(iParts[3]) : 20;

			final List<ORecord<?>> response = new ArrayList<ORecord<?>>();
			for (ORecord<?> rec : db.browseCluster(iParts[2])) {
				if (limit > 0 && response.size() >= limit)
					break;

				response.add(rec);
			}

			sendRecordsContent(response);
		} finally {
			if (db != null)
				OSharedDocumentDatabase.releaseDatabase(db);
		}
	}

	private void getDatabase(final String[] iParts) throws Exception {
		commandType = -1;

		checkSyntax(iParts, 2, "Syntax error: database/<database>");

		ODatabaseDocumentTx db = null;

		try {
			db = OSharedDocumentDatabase.acquireDatabase(iParts[1]);

			final StringWriter buffer = new StringWriter();
			final OJSONWriter json = new OJSONWriter(buffer);

			json.beginObject();
			if (db.getMetadata().getSchema().getClasses() != null) {
				json.beginCollection(1, false, "classes");
				for (OClass cls : db.getMetadata().getSchema().getClasses()) {
					json.beginObject(1, true, null);
					json.writeAttribute(2, true, "id", cls.getId());
					json.writeAttribute(2, true, "name", cls.getName());

					if (cls.properties() != null && cls.properties().size() > 0) {
						json.beginCollection(2, true, "properties");
						for (OProperty prop : cls.properties()) {
							json.beginObject(2, true, null);
							json.writeAttribute(3, true, "id", prop.getId());
							json.writeAttribute(3, true, "name", prop.getName());
							if (prop.getLinkedClass() != null)
								json.writeAttribute(3, true, "linkedClass", prop.getLinkedClass().getName());
							if (prop.getLinkedType() != null)
								json.writeAttribute(3, true, "linkedType", prop.getLinkedType());
							json.writeAttribute(3, true, "type", prop.getType().toString());
							json.writeAttribute(3, true, "min", prop.getMin());
							json.writeAttribute(3, true, "max", prop.getMax());
							json.endObject(2, true);
						}
						json.endCollection(1, true);
					}
					json.endObject(1, false);
				}
				json.endCollection(1, true);
			}

			if (db.getClusterNames() != null) {
				json.beginCollection(1, false, "clusters");
				for (String clusterName : db.getClusterNames()) {
					json.beginObject(2, true, null);
					json.writeAttribute(3, false, "id", db.getClusterIdByName(clusterName));
					json.writeAttribute(3, false, "name", clusterName);
					json.endObject(2, false);
				}
				json.endCollection(1, true);
			}
			json.endObject();

			sendTextContent(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, buffer.toString());
		} finally {
			if (db != null)
				OSharedDocumentDatabase.releaseDatabase(db);
		}
	}

	private void getClass(final String[] iParts) throws Exception {
		commandType = OChannelBinaryProtocol.COMMAND;

		checkSyntax(
				iParts,
				3,
				"Syntax error: class/<database>/<class-name>[/<limit>]<br/>Limit is optional and is setted to 20 by default. Set expressely to 0 to have no limits.");

		ODatabaseDocumentTx db = null;

		try {
			db = OSharedDocumentDatabase.acquireDatabase(iParts[1]);

			if (db.getMetadata().getSchema().getClass(iParts[2]) == null)
				throw new IllegalArgumentException("Invalid class '" + iParts[2] + "'");

			final int limit = iParts.length > 3 ? Integer.parseInt(iParts[3]) : 20;

			final List<ORecord<?>> response = new ArrayList<ORecord<?>>();
			for (ORecord<?> rec : db.browseClass(iParts[2])) {
				if (limit > 0 && response.size() >= limit)
					break;

				response.add(rec);
			}

			sendRecordsContent(response);
		} finally {
			if (db != null)
				OSharedDocumentDatabase.releaseDatabase(db);
		}
	}

	private void getServer(final String[] iParts) throws Exception {
		checkSyntax(iParts, 1, "Syntax error: server");

		try {
			StringWriter jsonBuffer = new StringWriter();
			OJSONWriter json = new OJSONWriter(jsonBuffer);

			json.beginObject();
			json.beginCollection(1, true, "connections");
			OClientConnection[] conns = OServerMain.server().getManagedServer().getConnections();
			for (OClientConnection c : conns) {
				json.beginObject();
				json.writeAttribute(2, true, "id", c.id);
				json.writeAttribute(2, true, "db", c.database != null ? c.database.getName() : null);
				json.writeAttribute(2, true, "protocol", c.protocol.getName());
				json.writeAttribute(2, true, "totalRequests", c.protocol.getTotalRequests());
				json.writeAttribute(2, true, "commandType", c.protocol.getCommandType());
				json.writeAttribute(2, true, "lastCommandType", c.protocol.getLastCommandType());
				json.writeAttribute(2, true, "lastCommandDetail", c.protocol.getLastCommandDetail());
				json.writeAttribute(2, true, "lastExecutionTime", c.protocol.getLastCommandExecutionTime());
				json.writeAttribute(2, true, "totalWorkingTime", c.protocol.getTotalWorkingTime());
				json.writeAttribute(2, true, "connectedOn", dateTimeFormat.format(new Date(c.since)));
				json.endObject();
			}
			json.endCollection(1, false);
			json.endObject();

			sendTextContent(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, jsonBuffer.toString());

		} finally {
		}
	}

	private void sendRecordsContent(final List<ORecord<?>> iRecords) throws IOException {
		StringBuilder buffer = new StringBuilder();
		buffer.append("{ \"result\": [\r\n");

		if (iRecords != null) {
			int counter = 0;
			String json;
			for (ORecord<?> rec : iRecords) {
				try {
					json = rec.toJSON();

					if (counter++ > 0)
						buffer.append(",\r\n");

					buffer.append(json);
				} catch (Exception e) {
				}
			}
		}

		buffer.append("] }");

		sendTextContent(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, buffer.toString());
	}

	private void sendRecordContent(final ORecord<?> iRecord) throws IOException {
		if (iRecord != null)
			sendTextContent(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, iRecord.toJSON("id,ver,class"));
	}

	private void checkSyntax(String[] iParts, final int iArgumentCount, final String iSyntax) {
		if (iParts.length < iArgumentCount)
			throw new IllegalArgumentException(iSyntax);
	}

	private void handleError(Exception e) {
		int errorCode = 500;

		if (e instanceof ORecordNotFoundException)
			errorCode = 404;
		else if (e instanceof OLockException)
			e = (Exception) e.getCause();

		final String msg = e.getMessage() != null ? e.getMessage() : "Internal error";

		try {
			sendTextContent(errorCode, "Error", OHttpUtils.CONTENT_TEXT_PLAIN, msg);
		} catch (IOException e1) {
			sendShutdown();
		}
	}
}
