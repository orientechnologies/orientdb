/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.common.parser.OVariableParser;
import com.orientechnologies.common.parser.OVariableParserListener;
import java.nio.file.Path;

public class OStorageVariableParser implements OVariableParserListener {
  private static final String STORAGE_PATH = "STORAGE_PATH";
  private final Path dbPath;
  private static final String VAR_BEGIN = "${";
  private static final String VAR_END = "}";

  public OStorageVariableParser(Path dbPath) {
    this.dbPath = dbPath;
  }

  public String resolveVariables(String iPath) {
    return (String) OVariableParser.resolveVariables(iPath, VAR_BEGIN, VAR_END, this);
  }

  @Override
  public String resolve(String variable) {
    if (variable.equals(STORAGE_PATH)) return dbPath.toString();

    String resolved = System.getProperty(variable);

    if (resolved == null)
      // TRY TO FIND THE VARIABLE BETWEEN SYSTEM'S ENVIRONMENT PROPERTIES
      resolved = System.getenv(variable);

    return resolved;
  }
}
