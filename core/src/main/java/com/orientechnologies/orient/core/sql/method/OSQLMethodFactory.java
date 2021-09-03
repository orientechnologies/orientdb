/*
 * Copyright 2013 Geomatys.
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
package com.orientechnologies.orient.core.sql.method;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import java.util.Set;

/** @author Johann Sorel (Geomatys) */
public interface OSQLMethodFactory {

  boolean hasMethod(String iName);

  /** @return Set of supported method names of this factory */
  Set<String> getMethodNames();

  /**
   * Create method for the given name. returned method may be a new instance each time or a
   * constant.
   *
   * @param name
   * @return OSQLMethod : created method
   * @throws OCommandExecutionException : when method creation fail
   */
  OSQLMethod createMethod(String name) throws OCommandExecutionException;
}
