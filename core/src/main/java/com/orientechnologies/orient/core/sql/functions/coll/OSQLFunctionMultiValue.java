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
package com.orientechnologies.orient.core.sql.functions.coll;

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Works against multi value objects like collections, maps and arrays.
 * 
 * @author Luca Garulli
 */
public class OSQLFunctionMultiValue extends OSQLFunctionAbstract {

  public static final String NAME = "multivalue";

  public OSQLFunctionMultiValue() {
    super(NAME, 2, -1);
  }

  @Override
  public Object execute(OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iFuncParams, OCommandContext iContext) {
    if (iFuncParams[0] == null || iFuncParams[1] == null)
      return null;

    if (iFuncParams.length == 2 && !OMultiValue.isMultiValue(iFuncParams[1]))
      return ODocumentHelper.getFieldValue(iFuncParams[0], iFuncParams[1].toString(), iContext);

    // MULTI VALUES
    final List<Object> list = new ArrayList<Object>();
    for (int i = 1; i < iFuncParams.length; ++i) {
      if (OMultiValue.isMultiValue(iFuncParams[i])) {
        for (Object o : OMultiValue.getMultiValueIterable(iFuncParams[i]))
          list.add(ODocumentHelper.getFieldValue(iFuncParams[0], o.toString(), iContext));
      } else
        list.add(ODocumentHelper.getFieldValue(iFuncParams[0], iFuncParams[i].toString(), iContext));
    }

    if (list.size() == 1)
      return list.get(0);

    return list;
  }

  @Override
  public String getSyntax() {
    return "multivalue(<value|expression|field>, <index>)";

  }
}
