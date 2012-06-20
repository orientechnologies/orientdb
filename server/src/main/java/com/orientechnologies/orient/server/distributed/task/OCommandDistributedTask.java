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
package com.orientechnologies.orient.server.distributed.task;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;

/**
 * Distributed task used for synchronization.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OCommandDistributedTask extends OAbstractDistributedTask<Object> {
  private static final long serialVersionUID = 1L;

  protected OCommandRequest command;

  public OCommandDistributedTask() {
  }

  public OCommandDistributedTask(final String nodeSource, final String databaseName, final EXECUTION_MODE iMode,
      final OCommandRequest iCommand) {
    super(nodeSource, databaseName, iMode);
    command = iCommand;
  }

  @Override
  public Object call() throws Exception {
    // ASSURE THE LOG IS CREATED
    ((ODistributedServerManager) OServerMain.server().getVariable("ODistributedAbstractPlugin")).getDatabaseSynchronizer(
        databaseName, nodeSource);

    OLogManager.instance().debug(this, "DISTRIBUTED <- command: %s", command.toString());

    Object result = command.execute();
    if (mode != EXECUTION_MODE.FIRE_AND_FORGET)
      return result;

    // FIRE AND FORGET MODE: AVOID THE PAYLOAD AS RESULT
    return null;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    super.writeExternal(out);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
  }

  @Override
  public String getName() {
    return "command";
  }
}
