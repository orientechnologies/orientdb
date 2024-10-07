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
package com.orientechnologies.tinkerpop.command;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostCommandGraph;
import java.util.Map;

public class OServerCommandPostCommandGremlin extends OServerCommandPostCommandGraph {
  public OServerCommandPostCommandGremlin() {}

  @Override
  protected OResultSet executeStatement(
      String language, String text, Object params, ODatabaseDocument db) {

    if ("gremlin".equalsIgnoreCase(language)) {

      OResultSet response = null;
      if (params instanceof Map) {
        response = db.execute("gremlin", text, (Map) params);
      } else if (params instanceof Object[]) {
        response = db.execute("gremlin", text, (Object[]) params);
      } else {
        response = db.execute("gremlin", text, (Map) null);
      }
      return response;

    } else {
      return super.executeStatement(language, text, params, db);
    }
  }
}
