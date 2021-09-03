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
package com.orientechnologies.orient.core.sql.functions.coll;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.sql.method.misc.OAbstractSQLMethod;
import java.util.ArrayList;
import java.util.List;

/**
 * Works against multi value objects like collections, maps and arrays.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLMethodMultiValue extends OAbstractSQLMethod {

  public static final String NAME = "multivalue";

  public OSQLMethodMultiValue() {
    super(NAME, 1, -1);
  }

  @Override
  public String getSyntax() {
    return "multivalue(<index>)";
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

    if (iParams.length == 1 && !OMultiValue.isMultiValue(iParams[0])) {
      return ODocumentHelper.getFieldValue(iThis, iParams[0].toString(), iContext);
    }

    // MULTI VALUES
    final List<Object> list = new ArrayList<Object>();
    for (int i = 0; i < iParams.length; ++i) {
      if (OMultiValue.isMultiValue(iParams[i])) {
        for (Object o : OMultiValue.getMultiValueIterable(iParams[i], false)) {
          list.add(ODocumentHelper.getFieldValue(iThis, o.toString(), iContext));
        }
      } else {
        list.add(ODocumentHelper.getFieldValue(iThis, iParams[i].toString(), iContext));
      }
    }

    if (list.size() == 1) {
      return list.get(0);
    }

    return list;
  }
}
