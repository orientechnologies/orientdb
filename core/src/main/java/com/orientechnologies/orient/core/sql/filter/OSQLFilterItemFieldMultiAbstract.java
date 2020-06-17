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
package com.orientechnologies.orient.core.sql.filter;

import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.query.OQueryRuntimeValueMulti;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents one or more object fields as value in the query condition.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OSQLFilterItemFieldMultiAbstract extends OSQLFilterItemAbstract {
  private List<String> names;
  private final OClass clazz;
  private final List<OCollate> collates = new ArrayList<OCollate>();

  public OSQLFilterItemFieldMultiAbstract(
      final OSQLPredicate iQueryCompiled,
      final String iName,
      final OClass iClass,
      final List<String> iNames) {
    super(iQueryCompiled, iName);
    names = iNames;
    clazz = iClass;

    for (String n : iNames) {
      collates.add(getCollateForField(iClass, n));
    }
  }

  public Object getValue(
      final OIdentifiable iRecord, Object iCurrentResult, OCommandContext iContext) {
    final ODocument doc = ((ODocument) iRecord);

    if (names.size() == 1)
      return transformValue(
          iRecord, iContext, ODocumentHelper.getIdentifiableValue(iRecord, names.get(0)));

    final String[] fieldNames = doc.fieldNames();
    final Object[] values = new Object[fieldNames.length];

    collates.clear();
    for (int i = 0; i < values.length; ++i) {
      values[i] = doc.field(fieldNames[i]);
      collates.add(getCollateForField(clazz, fieldNames[i]));
    }

    if (hasChainOperators()) {
      // TRANSFORM ALL THE VALUES
      for (int i = 0; i < values.length; ++i)
        values[i] = transformValue(iRecord, iContext, values[i]);
    }

    return new OQueryRuntimeValueMulti(this, values, collates);
  }
}
