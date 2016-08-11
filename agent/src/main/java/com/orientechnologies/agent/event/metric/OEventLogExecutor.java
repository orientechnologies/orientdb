/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */
package com.orientechnologies.agent.event.metric;

import com.orientechnologies.agent.event.OEventExecutor;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Map;

public abstract class OEventLogExecutor implements OEventExecutor {

  public boolean canExecute(ODocument source, ODocument when) {

    String levelType = when.field("type");

    String message = source.field("message");

    if (levelType != null && !levelType.isEmpty()) {
      if (levelType.equalsIgnoreCase(message)) {
        return true;
      }
      return false;
    }
    return false;

  }

  protected Map<String, Object> fillMapResolve(ODocument source, ODocument when) {
//    Map<String, Object> body2Name = new HashMap<String, Object>();
//    String server = source.field("server");
//
//    Date date = source.field("date");
//    body2Name.put("date", date);
//    if (server != null) {
//      body2Name.put("servername", server);
//
//    }
//    String metricName = source.field("name");
//    body2Name.put("metric", metricName);
//
//    String sourcelevel = (String) source.field("levelDescription");
//    body2Name.put("logvalue", sourcelevel);

    return source.toMap();
  }
}
