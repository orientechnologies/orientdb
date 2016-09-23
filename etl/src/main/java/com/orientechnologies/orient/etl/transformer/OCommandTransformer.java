/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.graph.gremlin.OCommandGremlin;

import static com.orientechnologies.orient.etl.OETLProcessor.LOG_LEVELS.*;
import static jdk.nashorn.internal.runtime.regexp.joni.Config.log;

/**
 * Executes a command.
 */
public class OCommandTransformer extends OAbstractTransformer {
  private String language = "sql";
  private String command;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters() + ","
        + "{language:{optional:true,description:'Command language, SQL by default'}},"
        + "{command:{optional:false,description:'Command to execute'}}],"
        + "input:['ODocument'],output:'ODocument'}");
  }

  @Override
  public void configure(final ODocument conf, final OCommandContext ctx) {
    super.configure(conf, ctx);

    if (conf.containsField("language"))
      language = conf.<String>field("language").toLowerCase();

    command = conf.field("command");
  }

  @Override
  public String getName() {
    return "command";
  }

  @Override
  public Object executeTransform(final Object input) {
    String runtimeCommand = (String) resolve(command);

    final OCommandRequest cmd;
    if (language.equals("sql")) {
      cmd = new OCommandSQL(runtimeCommand);


      log(DEBUG, "executing command=%s", runtimeCommand);
    } else if (language.equals("gremlin")) {
      cmd = new OCommandGremlin(runtimeCommand);
    } else {
      cmd = new OCommandScript(language, runtimeCommand);
    }
    cmd.setContext(context);

    log(DEBUG, "pipeline " + pipeline.getDocumentDatabase() );
    Object result = pipeline.getDocumentDatabase().command(cmd).execute();
    log(DEBUG, "executed command=%s, result=%s", cmd, result);
    return result;
  }
}
