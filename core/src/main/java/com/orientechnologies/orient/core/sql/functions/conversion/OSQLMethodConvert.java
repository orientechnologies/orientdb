/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.core.sql.functions.conversion;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.method.misc.OAbstractSQLMethod;
import java.util.Locale;

/**
 * Converts a value to another type in Java or OrientDB's supported types.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLMethodConvert extends OAbstractSQLMethod {

  public static final String NAME = "convert";

  public OSQLMethodConvert() {
    super(NAME, 1, 1);
  }

  @Override
  public String getSyntax() {
    return "convert(<type>)";
  }

  @Override
  public Object execute(
      Object iThis,
      OIdentifiable iCurrentRecord,
      OCommandContext iContext,
      Object ioResult,
      Object[] iParams) {
    if (iThis == null || iParams[0] == null) {
      return null;
    }

    final String destType = iParams[0].toString();

    if (destType.contains(".")) {
      try {
        return OType.convert(iThis, Class.forName(destType));
      } catch (ClassNotFoundException e) {
        OLogManager.instance().error(this, "Class for destination type was not found", e);
      }
    } else {
      final OType orientType = OType.valueOf(destType.toUpperCase(Locale.ENGLISH));
      if (orientType != null) {
        return OType.convert(iThis, orientType.getDefaultJavaType());
      }
    }

    return null;
  }
}
