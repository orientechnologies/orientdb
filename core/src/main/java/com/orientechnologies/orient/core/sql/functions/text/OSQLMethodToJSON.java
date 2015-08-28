/*
 * Copyright 2013 Orient Technologies.
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
package com.orientechnologies.orient.core.sql.functions.text;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.method.misc.OAbstractSQLMethod;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Converts a document in JSON string.
 * 
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLMethodToJSON extends OAbstractSQLMethod {

  public static final String NAME = "tojson";

  public OSQLMethodToJSON() {
    super(NAME, 0, 1);
  }

  @Override
  public String getSyntax() {
    return "toJSON([<format>])";
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, OCommandContext iContext, Object ioResult, Object[] iParams) {
    if (iThis == null) {
      return null;
    }

    final String format = iParams.length > 0 ? ((String) iParams[0]).replace("\"", "") : null;

    if (iThis instanceof ORecord) {

      final ORecord record = (ORecord) iThis;
      return iParams.length == 1 ? record.toJSON(format) : record.toJSON();

    } else if (iThis instanceof Map) {

      final ODocument doc = new ODocument().fromMap((Map<String, Object>) iThis);
      return iParams.length == 1 ? doc.toJSON(format) : doc.toJSON();

    } else if (OMultiValue.isMultiValue(iThis)) {

      final List<String> result = new ArrayList<String>();
      for (Object o : OMultiValue.getMultiValueIterable(iThis)) {
        if (o != null && o instanceof OIdentifiable) {
          final ORecord record = ((OIdentifiable) o).getRecord();
          result.add(iParams.length == 1 ? record.toJSON(format) : record.toJSON());
        }
      }
      return result;
    }
    return null;
  }
}
