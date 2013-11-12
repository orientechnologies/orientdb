/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.workbench.http;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.orientechnologies.workbench.OWorkbenchPlugin;
import com.orientechnologies.workbench.WUtils;
import com.orientechnologies.workbench.event.EventHelper;

public class OServerCommandMessageExecute extends OServerCommandAuthenticatedDbAbstract {
	private static final String[]	NAMES	= { "POST|message/*" };

	private OWorkbenchPlugin			monitor;

	public OServerCommandMessageExecute() {
	}

	public OServerCommandMessageExecute(final OServerCommandConfiguration iConfiguration) {
	}

	@Override
	public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
		if (monitor == null)
			monitor = OServerMain.server().getPluginByClass(OWorkbenchPlugin.class);

		final String[] parts = checkSyntax(iRequest.url, 3, "Syntax error: message/database/execute");

		iRequest.data.commandInfo = "Reset metrics";

		try {

			ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
			ODatabaseRecordThreadLocal.INSTANCE.set(db);
			ODocument message = new ODocument().fromJSON(iRequest.content);
			message.reload();
			String field = message.field("type");
			if ("chart".equals(field)) {
				String payload = message.field("payload");
				ODocument chart = new ODocument(OWorkbenchPlugin.CLASS_METRIC_CONFIG).fromJSON(payload);
				chart.save();
			} else if ("update".equals(field)) {

				String payload = message.field("payload");
				Proxy p = EventHelper.retrieveProxy(db);

				URL remoteUrl = new java.net.URL(payload);
				URLConnection urlConnection = null;
				if (p != null) {
					urlConnection = remoteUrl.openConnection(p);
				} else {
					urlConnection = remoteUrl.openConnection();
				}
				urlConnection.connect();
				File zip = WUtils.unpackArchive(remoteUrl, new File("download"));
				zip.delete();
			}

			message.field("type", "news");
			message.save();
			iResponse.send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, null, null);

		} catch (Exception e) {
			iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
		}
		return false;
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}
}
