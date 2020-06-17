/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandGetDictionary extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = {"GET|dictionary/*"};

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    iRequest.getData().commandInfo = "Dictionary lookup";

    String[] urlParts =
        checkSyntax(iRequest.getUrl(), 3, "Syntax error: dictionary/<database>/<key>");

    ODatabaseDocument db = null;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      final ORecord record = db.getDictionary().get(urlParts[2]);
      if (record == null)
        throw new ORecordNotFoundException(
            null, "Key '" + urlParts[2] + "' was not found in the database dictionary");

      iResponse.writeRecord(record);

    } finally {
      if (db != null) db.close();
    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
