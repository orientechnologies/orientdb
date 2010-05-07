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
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.enterprise.channel.text.OChannelTextServer;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolException;

public class ONetworkProtocolHttpDb extends ONetworkProtocolHttpAbstract {

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
				document(parts);
			else if (parts[0].equals("query"))
				query(parts);
			else if (parts[0].equals("dictionary"))
				dictionary(parts);
			else if (parts[0].equals("cluster"))
				cluster(parts);
			else if (parts[0].equals("class"))
				clazz(parts);
			else
				throw new IllegalArgumentException("Operation '" + parts[0] + "' not supported");

		} catch (Exception e) {
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

	private void document(final String[] iParts) throws Exception {
		checkSyntax(iParts, 3, "Syntax error: document/<database>/<record-id>");

		ODatabaseDocumentTx db = null;

		try {
			db = OSharedDocumentDatabase.acquireDatabase(iParts[1]);

			final ORecord<?> rec = db.load(new ORecordId(iParts[2]));

			sendRecordContent(rec);

		} finally {
			if (db != null)
				OSharedDocumentDatabase.releaseDatabase(db);
		}
	}

	private void dictionary(final String[] iParts) throws Exception {
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
	private void query(final String[] iParts) throws Exception {
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

	private void cluster(final String[] iParts) throws Exception {
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

	private void clazz(final String[] iParts) throws Exception {
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

	private void sendRecordsContent(final List<ORecord<?>> iRecords) throws IOException {
		StringBuilder buffer = new StringBuilder();
		buffer.append("{ \"result\": [\r\n");

		if (iRecords != null) {
			int counter = 0;
			for (ORecord<?> rec : iRecords) {
				if (counter++ > 0)
					buffer.append(",\r\n");

				buffer.append(rec.toJSON());
			}
		}

		buffer.append("] }");

		sendTextContent(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, buffer.toString());
	}

	private void sendRecordContent(final ORecord<?> iRecord) throws IOException {
		if (iRecord != null)
			sendTextContent(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, iRecord.toJSON());
	}

	@Override
	public void doPost(final String iURI, final String iRequest, final OChannelTextServer iChannel) throws ONetworkProtocolException {
	}

	@Override
	public void doPut(final String iURI, final String iRequest, final OChannelTextServer iChannel) throws ONetworkProtocolException {
	}

	@Override
	public void doDelete(final String iURI, final String iRequest, final OChannelTextServer iChannel)
			throws ONetworkProtocolException {
	}

	private void checkSyntax(String[] iParts, final int iArgumentCount, final String iSyntax) {
		if (iParts.length < iArgumentCount)
			throw new IllegalArgumentException(iSyntax);
	}
}
