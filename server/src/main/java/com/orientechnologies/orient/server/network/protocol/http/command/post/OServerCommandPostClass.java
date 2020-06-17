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
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandPostClass extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = {"POST|class/*"};

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    String[] urlParts =
        checkSyntax(iRequest.getUrl(), 3, "Syntax error: class/<database>/<class-name>");

    iRequest.getData().commandInfo = "Create class";
    iRequest.getData().commandDetail = urlParts[2];

    ODatabaseDocument db = null;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      if (db.getMetadata().getSchema().getClass(urlParts[2]) != null)
        throw new IllegalArgumentException("Class '" + urlParts[2] + "' already exists");

      db.getMetadata().getSchema().createClass(urlParts[2]);

      iResponse.send(
          OHttpUtils.STATUS_CREATED_CODE,
          OHttpUtils.STATUS_CREATED_DESCRIPTION,
          OHttpUtils.CONTENT_TEXT_PLAIN,
          db.getMetadata().getSchema().getClasses().size(),
          null);

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
