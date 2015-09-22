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

package com.orientechnologies.orient.etl.block;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.script.OCommandExecutorScript;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessor;

import java.util.HashMap;
import java.util.Map;

/**
 * Executes arbitrary code in any supported language by JVM.
 */
public class OCodeBlock extends OAbstractBlock {
  protected String                 language = "javascript";
  protected String                 code;
  protected OCommandExecutorScript cmd;
  protected Map<Object, Object>    params   = new HashMap<Object, Object>();

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[{language:{optional:true,description:'Code language, default is Javascript'}},"
        + "{code:{optional:false,description:'Code to execute'}}]," + "input:['Object'],output:'Object'}");
  }

  @Override
  public void configure(OETLProcessor iProcessor, final ODocument iConfiguration, OCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);
    if (iConfiguration.containsField("language"))
      language = iConfiguration.field("language");

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
  public Object executeBlock() {
    return cmd.executeInContext(context, params);
  }
}
