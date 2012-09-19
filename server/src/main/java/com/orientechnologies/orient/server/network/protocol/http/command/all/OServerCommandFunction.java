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
package com.orientechnologies.orient.server.network.protocol.http.command.all;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandFunction extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = { "GET|function/*", "POST|function/*" };

  @Override
  public boolean execute(final OHttpRequest iRequest) throws Exception {
    final String[] parts = checkSyntax(iRequest.url, 3, "Syntax error: function/<database>/<name>[/param]*");

    iRequest.data.commandInfo = "Execute a function";

    ODatabaseDocumentTx db = null;
    Object result = null;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      final OFunction f = db.getMetadata().getFunctionManager().getFunction(parts[2]);
      if (f == null)
        throw new IllegalArgumentException("Function '" + parts[2] + "' is not configured");

      final Object[] args = new Object[parts.length - 3];
      for (int i = 3; i < parts.length; ++i)
        args[i - 3] = parts[i];
      result = f.execute(args);

    } finally {
      if (db != null)
        OSharedDocumentDatabase.release(db);
    }

    if (result != null) {
      if (result instanceof ODocument && ((ODocument) result).isEmbedded()) {
        sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, null, OHttpUtils.CONTENT_JSON,
            ((ODocument) result).toJSON(), true, null);
      } else
        sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, null, OHttpUtils.CONTENT_TEXT_PLAIN,
            result, true, null);
    } else
      sendTextContent(iRequest, OHttpUtils.STATUS_OK_NOCONTENT_CODE, "", null, OHttpUtils.CONTENT_TEXT_PLAIN, null, true, null);

    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
