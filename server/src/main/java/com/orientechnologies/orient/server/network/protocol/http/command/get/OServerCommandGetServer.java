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

import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServerMain;
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
				json.beginObject();
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
				json.endObject();
			}
			json.endCollection(1, false);
			json.endObject();

			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, jsonBuffer.toString());

		} finally {
		}
	}

	public String[] getNames() {
		return NAMES;
	}
}
