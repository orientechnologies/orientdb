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
package com.orientechnologies.orient.core.sql.functions.misc;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import java.util.UUID;

/**
 * Generates a UUID as a 128-bits value using the Leach-Salz variant. For more information look at:
 * http://docs.oracle.com/javase/6/docs/api/java/util/UUID.html.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFunctionUUID extends OSQLFunctionAbstract {
  public static final String NAME = "uuid";

  /** Get the date at construction to have the same date for all the iteration. */
  public OSQLFunctionUUID() {
    super(NAME, 0, 0);
  }

  public Object execute(
      Object iThis,
      final OIdentifiable iCurrentRecord,
      final Object iCurrentResult,
      final Object[] iParams,
      OCommandContext iContext) {
    return UUID.randomUUID().toString();
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return false;
  }

  public String getSyntax() {
    return "uuid()";
  }

  @Override
  public Object getResult() {
    return null;
  }
}
