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
package com.orientechnologies.orient.server.network.protocol.http.command.delete;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;

public class OServerCommandDeleteIndex extends OServerCommandDocumentAbstract {
  private static final String[] NAMES = { "DELETE|index/*" };

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: index/<database>/<index-name>/<key>/[<value>]");

    iRequest.data.commandInfo = "Index remove";

    ODatabaseDocument db = null;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      final OIndex<?> index = db.getMetadata().getIndexManager().getIndex(urlParts[2]);
      if (index == null)
        throw new IllegalArgumentException("Index name '" + urlParts[2] + "' not found");

      final boolean found;
      if (urlParts.length > 4)
        found = index.remove(urlParts[3], new ORecordId(urlParts[3]));
      else
        found = index.remove(urlParts[3]);

      if (found)
        iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, null, null);
      else
        iResponse.send(OHttpUtils.STATUS_NOTFOUND_CODE, OHttpUtils.STATUS_NOTFOUND_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
            null, null);
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
