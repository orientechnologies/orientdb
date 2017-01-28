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
import com.orientechnologies.orient.core.command.script.OCommandExecutorScript;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

/**
 * Executes arbitrary code in any supported language by JVM.
 */
public class OETLCodeTransformer extends OETLAbstractTransformer {
  private final Map<Object, Object> params   = new HashMap<Object, Object>();
  private       String              language = "javascript";
  private OCommandExecutorScript cmd;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters() + ","
        + "{language:{optional:true,description:'Code language, default is Javascript'}},"
        + "{code:{optional:false,description:'Code to execute'}}" + "]}");
  }

  @Override
  public void configure(final ODocument iConfiguration, OCommandContext iContext) {
    super.configure(iConfiguration, iContext);
    if (iConfiguration.containsField("language"))
      language = iConfiguration.field("language");

    String code;
    if (iConfiguration.containsField("code"))
      code = iConfiguration.field("code");
    else
      throw new IllegalArgumentException("'code' parameter is mandatory in Code Transformer");

    cmd = new OCommandExecutorScript().parse(new OCommandScript(language, code));
  }

  @Override
  public String getName() {
    return "code";
  }

  @Override
  public Object executeTransform(ODatabaseDocument db, final Object input) {
    if (input == null)
      return null;

    params.put("input", input);
    if (input instanceof OIdentifiable)
      params.put("record", ((OIdentifiable) input).getRecord());

    try {
      Object result = cmd.executeInContext(context, params);

      log(Level.FINE, "executed code=%s, result=%s", cmd, result);
      return result;

    } catch (Exception e) {

      log(Level.SEVERE, "exception=%s - input=%s - command=%s ", e.getMessage(), input, cmd);

      throw e;
    }

  }
}
