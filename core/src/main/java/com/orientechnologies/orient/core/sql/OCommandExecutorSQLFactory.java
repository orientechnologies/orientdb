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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import java.util.Set;

/**
 * Factory to register new OCommandExecutorSQL.
 *
 * @author Johann Sorel (Geomatys)
 */
public interface OCommandExecutorSQLFactory {

  /** @return Set of supported command names of this factory */
  Set<String> getCommandNames();

  /**
   * Create command for the given name. returned command may be a new instance each time or a
   * constant.
   *
   * @param name
   * @return OCommandExecutorSQLAbstract : created command
   * @throws OCommandExecutionException : when command creation fail
   */
  OCommandExecutor createCommand(String name) throws OCommandExecutionException;
}
