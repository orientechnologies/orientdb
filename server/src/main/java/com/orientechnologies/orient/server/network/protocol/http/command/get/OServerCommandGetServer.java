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
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfiler.OProfilerEntry;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerEntryConfiguration;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

public class OServerCommandGetServer extends OServerCommandAuthenticatedServerAbstract {
	private static final String[]		NAMES						= { "GET|server" };
	private final static DateFormat	dateTimeFormat	= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	public OServerCommandGetServer() {
		super("info-server");
	}

	@Override
	public boolean execute(final OHttpRequest iRequest) throws Exception {
		checkSyntax(iRequest.url, 1, "Syntax error: server");

		iRequest.data.commandInfo = "Server status";

		try {
			StringWriter jsonBuffer = new StringWriter();
			OJSONWriter json = new OJSONWriter(jsonBuffer);

			json.beginObject();

			json.beginCollection(1, true, "connections");

			String lastCommandOn;
			String connectedOn;

			final OClientConnection[] conns = OServerMain.server().getManagedServer().getConnections();
			for (OClientConnection c : conns) {
				synchronized (dateTimeFormat) {
					lastCommandOn = dateTimeFormat.format(new Date(c.protocol.getData().lastCommandReceived));
					connectedOn = dateTimeFormat.format(new Date(c.since));
				}

				json.beginObject(2);
				writeField(json, 2, "id", c.id);
				writeField(json, 2, "id", c.id);
				writeField(json, 2, "remoteAddress", c.protocol.getChannel() != null ? c.protocol.getChannel().toString() : "Disconnected");
				writeField(json, 2, "db", c.database != null ? c.database.getName() : "-");
				writeField(json, 2, "user", c.database != null && c.database.getUser() != null ? c.database.getUser().getName() : "-");
				writeField(json, 2, "protocol", c.protocol.getName());
				writeField(json, 2, "totalRequests", c.protocol.getData().totalRequests);
				writeField(json, 2, "commandInfo", c.protocol.getData().commandInfo);
				writeField(json, 2, "commandDetail", c.protocol.getData().commandDetail);
				writeField(json, 2, "lastCommandOn", lastCommandOn);
				writeField(json, 2, "lastCommandInfo", c.protocol.getData().lastCommandInfo);
				writeField(json, 2, "lastCommandDetail", c.protocol.getData().lastCommandDetail);
				writeField(json, 2, "lastExecutionTime", c.protocol.getData().lastCommandExecutionTime);
				writeField(json, 2, "totalWorkingTime", c.protocol.getData().totalCommandExecutionTime);
				writeField(json, 2, "connectedOn", connectedOn);
				json.endObject(2);
			}
			json.endCollection(1, false);

			json.beginCollection(1, true, "dbs");
			Map<String, OResourcePool<String, ODatabaseDocumentTx>> dbPool = OSharedDocumentDatabase.getDatabasePools();
			for (Entry<String, OResourcePool<String, ODatabaseDocumentTx>> entry : dbPool.entrySet()) {
				for (ODatabaseDocumentTx db : entry.getValue().getResources()) {

					json.beginObject(2);
					writeField(json, 2, "db", db.getName());
					writeField(json, 2, "user", db.getUser() != null ? db.getUser().getName() : "-");
					writeField(json, 2, "open", db.isClosed() ? "closed" : "open");
					writeField(json, 2, "storage", db.getStorage().getClass().getSimpleName());
					json.endObject(2);
				}
			}
			json.endCollection(1, false);

			json.beginCollection(1, true, "storages");
			OStorage[] storages = OServerMain.server().getManagedServer().getOpenedStorages();
			for (OStorage s : storages) {
				json.beginObject(2);
				writeField(json, 2, "name", s.getName());
				writeField(json, 2, "type", s.getClass().getSimpleName());
				writeField(json, 2, "path", s instanceof OStorageLocal ? ((OStorageLocal) s).getStoragePath().replace('\\', '/') : "");
				writeField(json, 2, "activeUsers", s.getUsers());
				json.endObject(2);
			}
			json.endCollection(1, false);

			json.beginCollection(2, true, "properties");
			for (OServerEntryConfiguration entry : OServerMain.server().getConfiguration().properties) {
				json.beginObject(3, true, null);
				json.writeAttribute(4, false, "name", entry.name);
				json.writeAttribute(4, false, "value", entry.value);
				json.endObject(3, true);
			}
			json.endCollection(2, true);

			json.beginObject(1, true, "profiler");
			json.beginCollection(2, true, "hookValues");
			for (String h : OProfiler.getInstance().getHooks()) {
				json.beginObject(3);
				writeField(json, 3, "name", h);
				writeField(json, 3, "value", OProfiler.getInstance().getHookValue(h));
				json.endObject(3);
			}
			json.endCollection(2, false);
			
			json.beginCollection(2, true, "counters");
			for (String c : OProfiler.getInstance().getCounters()) {
				json.beginObject(3);
				writeField(json, 3, "name", c);
				writeField(json, 3, "value", OProfiler.getInstance().getCounter(c));
				json.endObject(3);
			}
			json.endCollection(2, false);

			OProfilerEntry pEntry;

			json.beginCollection(2, true, "stats");
			for (String s : OProfiler.getInstance().getStats()) {
				pEntry = OProfiler.getInstance().getStat(s);

				json.beginObject(3);
				writeField(json, 3, "name", s);
				writeField(json, 3, "total", pEntry.items);
				writeField(json, 3, "averageElapsed", pEntry.average);
				writeField(json, 3, "minElapsed", pEntry.min);
				writeField(json, 3, "maxElapsed", pEntry.max);
				writeField(json, 3, "lastElapsed", pEntry.last);
				writeField(json, 3, "totalElapsed", pEntry.total);
				json.endObject(3);
			}
			json.endCollection(2, false);

			json.beginCollection(2, true, "chronos");
			for (String c : OProfiler.getInstance().getChronos()) {
				pEntry = OProfiler.getInstance().getChrono(c);

				json.beginObject(3);
				writeField(json, 3, "name", c);
				writeField(json, 3, "total", pEntry.items);
				writeField(json, 3, "averageElapsed", pEntry.average);
				writeField(json, 3, "minElapsed", pEntry.min);
				writeField(json, 3, "maxElapsed", pEntry.max);
				writeField(json, 3, "lastElapsed", pEntry.last);
				writeField(json, 3, "totalElapsed", pEntry.total);
				json.endObject(3);
			}
			json.endCollection(2, false);
			json.endObject(1);

			json.endObject();

			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_JSON, jsonBuffer.toString());

		} finally {
		}
		return false;
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}

	private void writeField(final OJSONWriter json, final int iLevel, final String iAttributeName, final Object iAttributeValue)
			throws IOException {
		json.writeAttribute(iLevel, true, iAttributeName, iAttributeValue != null ? iAttributeValue.toString() : "-");
	}
}
