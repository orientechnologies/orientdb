/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl.transformer;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/** Executes a command. */
public class OETLCommandTransformer extends OETLAbstractTransformer {
  private String language = "sql";
  private String command;
  private boolean returnInput = false;

  @Override
  public ODocument getConfiguration() {
    return new ODocument()
        .fromJSON(
            "{parameters:["
                + getCommonConfigurationParameters()
                + ","
                + "{language:{optional:true,description:'Command language, SQL by default'}},"
                + "{command:{optional:false,description:'Command to execute'}}],"
                + "input:['ODocument'],output:'ODocument'}");
  }

  @Override
  public void configure(final ODocument conf, final OCommandContext ctx) {
    super.configure(conf, ctx);

    if (conf.containsField("language"))
      language = conf.<String>field("language").toLowerCase(Locale.ENGLISH);

    command = conf.field("command");
    returnInput = Boolean.TRUE.equals(conf.field("returnInput"));
  }

  @Override
  public String getName() {
    return "command";
  }

  @Override
  public Object executeTransform(ODatabaseDocument db, Object input) {
    String runtimeCommand = (String) resolve(command);

    OResultSet result = db.execute(language, runtimeCommand);
    List<OElement> finalResult =
        result.stream().map(x -> x.toElement()).collect(Collectors.toList());
    result.close();

    if (returnInput) {
      if (input instanceof OElement) {
        input = db.reload(((OElement) input).getRecord(), null, true);
      }
      return input;
    } else {
      return finalResult;
    }
  }
}
