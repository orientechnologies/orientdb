/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.distributed.sql;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Distributed related command operator executor factory. It's auto-discovered.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ODistributedCommandExecutorSQLFactory implements OCommandExecutorSQLFactory {

  private static final Map<String, Class<? extends OCommandExecutorSQLAbstract>> COMMANDS;

  static {

    // COMMANDS
    final Map<String, Class<? extends OCommandExecutorSQLAbstract>> commands =
        new HashMap<String, Class<? extends OCommandExecutorSQLAbstract>>();

    commands.put(OCommandExecutorSQLHASyncDatabase.NAME, OCommandExecutorSQLHASyncDatabase.class);
    commands.put(OCommandExecutorSQLHASyncCluster.NAME, OCommandExecutorSQLHASyncCluster.class);
    commands.put(OCommandExecutorSQLHARemoveServer.NAME, OCommandExecutorSQLHARemoveServer.class);
    commands.put(OCommandExecutorSQLHAStatus.NAME, OCommandExecutorSQLHAStatus.class);

    COMMANDS = Collections.unmodifiableMap(commands);
  }

  /** {@inheritDoc} */
  public Set<String> getCommandNames() {
    return COMMANDS.keySet();
  }

  /** {@inheritDoc} */
  public OCommandExecutorSQLAbstract createCommand(final String name)
      throws OCommandExecutionException {
    final Class<? extends OCommandExecutorSQLAbstract> clazz = COMMANDS.get(name);

    if (clazz == null) {
      throw new OCommandExecutionException("Unknown command name :" + name);
    }

    try {
      return clazz.newInstance();
    } catch (Exception e) {
      throw OException.wrapException(
          new OCommandExecutionException(
              "Error in creation of command "
                  + name
                  + "(). Probably there is not an empty constructor or the constructor generates errors"),
          e);
    }
  }
}
