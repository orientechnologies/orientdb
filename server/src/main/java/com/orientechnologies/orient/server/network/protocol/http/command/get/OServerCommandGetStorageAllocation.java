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
import java.util.Collections;
import java.util.List;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.impl.local.ODataLocal;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandGetStorageAllocation extends OServerCommandAuthenticatedDbAbstract {
	private static final String[]	NAMES	= { "GET|allocation/*" };

	@Override
	public boolean execute(final OHttpRequest iRequest) throws Exception {
		String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: allocation/<database>");

		iRequest.data.commandInfo = "Storage allocation";
		iRequest.data.commandDetail = urlParts[2];

		ODatabaseDocumentTx db = null;

		try {
			db = getProfiledDatabaseInstance(iRequest);

			final List<OPhysicalPosition> holes = ((OStorageLocal) db.getStorage()).getHoles();
			Collections.sort(holes);

			final StringWriter buffer = new StringWriter();
			final OJSONWriter json = new OJSONWriter(buffer);

			json.beginObject();

			long current = 0;

			for (OPhysicalPosition h : holes) {
				if (current < h.dataPosition) {
					// DATA SEGMENT
					json.beginObject(1, true, null);
					json.writeAttribute(2, false, "type", "d");
					json.writeAttribute(2, false, "offset", current);
					json.writeAttribute(2, false, "size", h.dataPosition - current);
					json.endObject(1);
				}

				json.beginObject(1, true, null);
				json.writeAttribute(2, false, "type", "h");
				json.writeAttribute(2, false, "offset", h.dataPosition);
				json.writeAttribute(2, false, "size", h.recordSize + ODataLocal.RECORD_FIX_SIZE);
				json.endObject(1);

				current = h.dataPosition + h.recordSize + ODataLocal.RECORD_FIX_SIZE;
			}

			json.endObject();
			json.flush();

			sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_JSON, buffer.toString());
		} finally {
			if (db != null)
				OSharedDocumentDatabase.release(db);
		}
		return false;
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}
}
