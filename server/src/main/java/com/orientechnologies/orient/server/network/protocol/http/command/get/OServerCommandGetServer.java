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

import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfiler.Chrono;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;

public class OServerCommandGetServer extends OServerCommandAbstract {
	private static final String[]		NAMES						= { "GET.server" };
	private final static DateFormat	dateTimeFormat	= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	public void execute(final OHttpRequest iRequest) throws Exception {
		checkSyntax(iRequest.url, 1, "Syntax error: server");

		try {
			StringWriter jsonBuffer = new StringWriter();
			OJSONWriter json = new OJSONWriter(jsonBuffer);

			json.beginObject();

			json.beginCollection(1, true, "connections");
			OClientConnection[] conns = OServerMain.server().getManagedServer().getConnections();
			for (OClientConnection c : conns) {
				json.beginObject(2);
				json.writeAttribute(2, true, "id", c.id);
				json.writeAttribute(2, true, "db", c.database != null ? c.database.getName() : null);
				json.writeAttribute(2, true, "protocol", c.protocol.getName());
				json.writeAttribute(2, true, "totalRequests", c.protocol.getData().totalRequests);
				json.writeAttribute(2, true, "commandType", c.protocol.getData().commandType);
				json.writeAttribute(2, true, "lastCommandType", c.protocol.getData().lastCommandType);
				json.writeAttribute(2, true, "lastCommandDetail", c.protocol.getData().lastCommandDetail);
				json.writeAttribute(2, true, "lastExecutionTime", c.protocol.getData().lastCommandExecutionTime);
				json.writeAttribute(2, true, "totalWorkingTime", c.protocol.getData().totalCommandExecutionTime);
				json.writeAttribute(2, true, "connectedOn", dateTimeFormat.format(new Date(c.since)));
				json.endObject(2);
			}
			json.endCollection(1, false);

			json.beginCollection(1, true, "dbs");
			Map<String, OResourcePool<String, ODatabaseDocumentTx>> dbPool = OSharedDocumentDatabase.getDatabasePools();
			for (Entry<String, OResourcePool<String, ODatabaseDocumentTx>> entry : dbPool.entrySet()) {
				for (ODatabaseDocumentTx db : entry.getValue().getResources()) {

					json.beginObject(2);
					json.writeAttribute(2, true, "db", db.getName());
					json.writeAttribute(2, true, "open", !db.isClosed());
					json.writeAttribute(2, true, "storage", db.getStorage().getClass().getSimpleName());
					json.endObject(2);
				}
			}
			json.endCollection(1, false);

			json.beginCollection(1, true, "storages");
			OStorage[] storages = OServerMain.server().getManagedServer().getOpenedStorages();
			for (OStorage s : storages) {
				json.beginObject(2);
				json.writeAttribute(2, true, "name", s.getName());
				json.writeAttribute(2, true, "type", s.getClass().getSimpleName());
				json.writeAttribute(2, true, "path", s instanceof OStorageLocal ? ((OStorageLocal) s).getStoragePath() : "");
				json.writeAttribute(2, true, "activeUsers", s.getUsers());
				json.endObject(2);
			}
			json.endCollection(1, false);

			json.beginObject(1, true, "profiler");
			json.beginCollection(2, true, "stats");
			for (Entry<String, Long> s : OProfiler.getInstance().getStatistics()) {
				json.beginObject(3);
				json.writeAttribute(3, true, "name", s.getKey());
				json.writeAttribute(3, true, "value", s.getValue());
				json.endObject(3);
			}
			json.endCollection(2, false);
			json.beginCollection(2, true, "chronos");
			for (Entry<String, Chrono> c : OProfiler.getInstance().getChronos()) {
				json.beginObject(3);
				json.writeAttribute(3, true, "name", c.getKey());
				json.writeAttribute(3, true, "total", c.getValue().items);
				json.writeAttribute(3, true, "averageElapsed", c.getValue().averageElapsed);
				json.writeAttribute(3, true, "minElapsed", c.getValue().minElapsed);
				json.writeAttribute(3, true, "maxElapsed", c.getValue().maxElapsed);
				json.writeAttribute(3, true, "lastElapsed", c.getValue().lastElapsed);
				json.writeAttribute(3, true, "totalElapsed", c.getValue().totalElapsed);
				json.endObject(3);
			}
			json.endCollection(2, false);
			json.endObject(1);

			json.endObject();

			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_TEXT_PLAIN, jsonBuffer.toString());

		} finally {
		}
	}

	public String[] getNames() {
		return NAMES;
	}
}
