/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(-at-)orientdb.com)
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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Live Query command operator executor factory.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OLiveCommandExecutorSQLFactory implements OCommandExecutorSQLFactory {

  private static Map<String, Class<? extends OCommandExecutorSQLAbstract>> COMMANDS =
      new HashMap<String, Class<? extends OCommandExecutorSQLAbstract>>();

  static {
    init();
  }

  public static void init() {
    if (COMMANDS.size() == 0) {
      synchronized (OLiveCommandExecutorSQLFactory.class) {
        if (COMMANDS.size() == 0) {
          final Map<String, Class<? extends OCommandExecutorSQLAbstract>> commands =
              new HashMap<String, Class<? extends OCommandExecutorSQLAbstract>>();
          commands.put(
              OCommandExecutorSQLLiveSelect.KEYWORD_LIVE_SELECT,
              OCommandExecutorSQLLiveSelect.class);
          commands.put(
              OCommandExecutorSQLLiveUnsubscribe.KEYWORD_LIVE_UNSUBSCRIBE,
              OCommandExecutorSQLLiveUnsubscribe.class);

          COMMANDS = Collections.unmodifiableMap(commands);
        }
      }
    }
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
      throw new OCommandExecutionException("Unknowned command name :" + name);
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
