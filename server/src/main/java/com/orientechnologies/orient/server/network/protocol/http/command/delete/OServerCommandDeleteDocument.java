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
package com.orientechnologies.orient.server.network.protocol.http.command.delete;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;

public class OServerCommandDeleteDocument extends OServerCommandDocumentAbstract {
  private static final String[] NAMES = { "DELETE|document/*" };

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    ODatabaseDocumentTx db = null;

    try {
      final String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: document/<database>/<record-id>");

      iRequest.data.commandInfo = "Delete document";

      db = getProfiledDatabaseInstance(iRequest);

      // PARSE PARAMETERS
      final int parametersPos = urlParts[2].indexOf('?');
      final String rid = parametersPos > -1 ? urlParts[2].substring(0, parametersPos) : urlParts[2];
      final ORecordId recordId = new ORecordId(rid);

      if (!recordId.isValid())
        throw new IllegalArgumentException("Invalid Record ID in request: " + urlParts[2]);

      final ODocument doc = new ODocument(recordId);

      // UNMARSHALL DOCUMENT WITH REQUEST CONTENT
      if (iRequest.content != null)
        // GET THE VERSION FROM THE DOCUMENT
        doc.fromJSON(iRequest.content);
      else {
        if (iRequest.ifMatch != null)
          // USE THE IF-MATCH HTTP HEADER AS VERSION
          doc.getRecordVersion().getSerializer().fromString(iRequest.ifMatch, doc.getRecordVersion());
        else
          // IGNORE THE VERSION
          doc.getRecordVersion().disable();
      }
      doc.delete();

      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, null, null);

    } finally {
      if (db != null)
        db.close();
    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
