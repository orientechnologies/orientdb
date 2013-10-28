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
package com.orientechnologies.orient.server.network.protocol.http.command.put;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;

public class OServerCommandPutDocument extends OServerCommandDocumentAbstract {
  private static final String[] NAMES = { "PUT|document/*" };

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 2,
        "Syntax error: document/<database>[/<record-id>][?updateMode=full|partial]");

    iRequest.data.commandInfo = "Edit Document";

    ODatabaseDocumentTx db = null;
    ORecordId recordId = null;
    final ODocument doc;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      if (urlParts.length > 2) {
        // EXTRACT RID
        final int parametersPos = urlParts[2].indexOf('?');
        final String rid = parametersPos > -1 ? urlParts[2].substring(0, parametersPos) : urlParts[2];
        recordId = new ORecordId(rid);

        if (!recordId.isValid())
          throw new IllegalArgumentException("Invalid Record ID in request: " + recordId);
      } else
        recordId = new ORecordId();

      // UNMARSHALL DOCUMENT WITH REQUEST CONTENT
      doc = new ODocument();
      doc.fromJSON(iRequest.content);

      if (!recordId.isValid())
        recordId = (ORecordId) doc.getIdentity();
      else
        doc.setIdentity(recordId);

      if (!recordId.isValid())
        throw new IllegalArgumentException("Invalid Record ID in request: " + recordId);

      final ODocument currentDocument = db.load(recordId);

      if (currentDocument == null) {
        iResponse.send(OHttpUtils.STATUS_NOTFOUND_CODE, OHttpUtils.STATUS_NOTFOUND_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
            "Record " + recordId + " was not found.", null);
        return false;
      }

      boolean partialUpdateMode = false;
      String mode = iRequest.getParameter("updateMode");
      if (mode != null && mode.equalsIgnoreCase("partial"))
        partialUpdateMode = true;

      mode = iRequest.getHeader("updateMode");
      if (mode != null && mode.equalsIgnoreCase("partial"))
        partialUpdateMode = true;

      currentDocument.merge(doc, partialUpdateMode, false);
      currentDocument.getRecordVersion().copyFrom(doc.getRecordVersion());

      currentDocument.save();

    } finally {
      if (db != null)
        db.close();
    }

    iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Record " + recordId + " updated successfully.",
        null);
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
