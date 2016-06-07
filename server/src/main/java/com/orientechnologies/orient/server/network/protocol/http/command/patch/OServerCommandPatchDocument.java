/*
   *
   *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
   *  *
   *  *  Licensed under the Apache License, Version 2.0 (the "License");
   *  *  you may not use this file except in compliance with the License.
   *  *  You may obtain a copy of the License at
   *  *
   *  *       http://www.apache.org/licenses/LICENSE-2.0
   *  *
   *  *  Unless required by applicable law or agreed to in writing, software
   *  *  distributed under the License is distributed on an "AS IS" BASIS,
   *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   *  *  See the License for the specific language governing permissions and
   *  *  limitations under the License.
   *  *
   *  * For more information: http://www.orientechnologies.com
   *
   */
package com.orientechnologies.orient.server.network.protocol.http.command.patch;

import com.orientechnologies.orient.core.record.ORecordInternal;

public class OServerCommandPatchDocument
    extends com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract {
  private static final String[] NAMES = { "PATCH|document/*" };

  @Override
  public boolean execute(final com.orientechnologies.orient.server.network.protocol.http.OHttpRequest iRequest,
      com.orientechnologies.orient.server.network.protocol.http.OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: document/<database>[/<record-id>]");

    iRequest.data.commandInfo = "Edit Document";

    com.orientechnologies.orient.core.db.document.ODatabaseDocument db = null;
    com.orientechnologies.orient.core.id.ORecordId recordId;
    final com.orientechnologies.orient.core.record.impl.ODocument doc;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      if (urlParts.length > 2) {
        // EXTRACT RID
        final int parametersPos = urlParts[2].indexOf('?');
        final String rid = parametersPos > -1 ? urlParts[2].substring(0, parametersPos) : urlParts[2];
        recordId = new com.orientechnologies.orient.core.id.ORecordId(rid);

        if (!recordId.isValid())
          throw new IllegalArgumentException("Invalid Record ID in request: " + recordId);
      } else
        recordId = new com.orientechnologies.orient.core.id.ORecordId();

      // UNMARSHALL DOCUMENT WITH REQUEST CONTENT
      doc = new com.orientechnologies.orient.core.record.impl.ODocument();
      doc.fromJSON(iRequest.content);

      if (iRequest.ifMatch != null)
        // USE THE IF-MATCH HTTP HEADER AS VERSION
        ORecordInternal.setVersion(doc, Integer.parseInt(iRequest.ifMatch));

      if (!recordId.isValid())
        recordId = (com.orientechnologies.orient.core.id.ORecordId) doc.getIdentity();
      else
        ORecordInternal.setIdentity(doc, recordId);

      if (!recordId.isValid())
        throw new IllegalArgumentException("Invalid Record ID in request: " + recordId);

      final com.orientechnologies.orient.core.record.impl.ODocument currentDocument = db.load(recordId);

      if (currentDocument == null) {
        iResponse.send(com.orientechnologies.orient.server.network.protocol.http.OHttpUtils.STATUS_NOTFOUND_CODE,
            com.orientechnologies.orient.server.network.protocol.http.OHttpUtils.STATUS_NOTFOUND_DESCRIPTION,
            com.orientechnologies.orient.server.network.protocol.http.OHttpUtils.CONTENT_TEXT_PLAIN,
            "Record " + recordId + " was not found.", null);
        return false;
      }

      boolean partialUpdateMode = true;
      currentDocument.merge(doc, partialUpdateMode, false);
      ORecordInternal.setVersion(currentDocument, doc.getVersion());

      currentDocument.save();

      iResponse.send(com.orientechnologies.orient.server.network.protocol.http.OHttpUtils.STATUS_OK_CODE,
          com.orientechnologies.orient.server.network.protocol.http.OHttpUtils.STATUS_OK_DESCRIPTION,
          com.orientechnologies.orient.server.network.protocol.http.OHttpUtils.CONTENT_TEXT_PLAIN, currentDocument.toJSON(),
          com.orientechnologies.orient.server.network.protocol.http.OHttpUtils.HEADER_ETAG + doc.getVersion());

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
