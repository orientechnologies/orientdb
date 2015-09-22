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

import com.orientechnologies.orient.console.OConsoleDatabaseApp;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessor;

import java.util.List;

/**
 * Executes the OrientDB console. Useful to execute batches.
 */
public class OConsoleBlock extends OAbstractBlock {
  protected String              file;
  protected List<String>        commands;
  protected OConsoleDatabaseApp console;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters()
        + "{file:{optional:true,description:'Input filename with commands to execute'}}"
        + "{commands:{optional:true,description:'Commands to execute in sequence as an array of strings'}}" + "]}");
  }

  @Override
  public void configure(OETLProcessor iProcessor, final ODocument iConfiguration, OCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);
    if (iConfiguration.containsField("file"))
      file = iConfiguration.field("file");

    if (iConfiguration.containsField("commands"))
      commands = iConfiguration.field("commands");

    if (file == null && commands == null)
      throw new OConfigurationException("file or commands are mandatory");

    if (file != null)
      console = new OConsoleDatabaseApp(new String[] { file });
    else
      console = new OConsoleDatabaseApp(commands.toArray(new String[commands.size()]));
  }

  @Override
  public String getName() {
    return "console";
  }

  @Override
  public Object executeBlock() {
    return console.run();
  }
}
