/*
 * Copyright 2014 Orient Technologies.
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

package com.orientechnologies.lucene.functions;

import com.orientechnologies.lucene.collections.OSpatialCompositeKey;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionMultiValueAbstract;

import java.util.*;

public class OLuceneNearFunction extends OSQLFunctionMultiValueAbstract<Set<Object>> {

  public static final String NAME = "near";

  public OLuceneNearFunction() {
    super(NAME, 5, 6);
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext) {

    String clazz = (String) iParams[0];
    String latField = (String) iParams[1];
    String lngField = (String) iParams[2];
    ODatabaseDocument databaseRecord = ODatabaseRecordThreadLocal.INSTANCE.get();
    Set<OIndex<?>> indexes = databaseRecord.getMetadata().getSchema().getClass(clazz).getInvolvedIndexes(latField, lngField);
    for (OIndex i : indexes) {
      if (OClass.INDEX_TYPE.SPATIAL.toString().equals(i.getInternal().getType())) {
        List<Object> params = new ArrayList<Object>();
        params.add(iParams[3]);
        params.add(iParams[4]);
        double distance = iParams.length > 5 ? ((Number) iParams[5]).doubleValue() : 0;
        Object ret = i.get(new OSpatialCompositeKey(params).setMaxDistance(distance));
        if (ret instanceof Collection) {
          if (context == null)
            context = new HashSet<Object>();
          context.addAll((Collection<?>) ret);
        }
        return ret;
      }
    }
    return null;
  }

  @Override
  public String getSyntax() {
    return "near(<class>,<field-x>,<field-y>,<x-value>,<y-value>[,<maxDistance>])";
  }
}
