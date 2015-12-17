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
package com.orientechnologies.orient.core.sql.method.misc;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLMethodField extends OAbstractSQLMethod {

  public static final String NAME = "field";

  public OSQLMethodField() {
    super(NAME, 1, 1);
  }

  @Override
  public Object execute(Object iThis, final OIdentifiable iCurrentRecord, final OCommandContext iContext, Object ioResult,
      final Object[] iParams) {
    if (iParams[0] == null)
      return null;

    final String paramAsString = iParams[0].toString();

    if (ioResult != null) {
      if (ioResult instanceof String) {
        try {
          ioResult = new ODocument(new ORecordId((String) ioResult));
        } catch (Exception e) {
          OLogManager.instance().error(this, "Error on reading rid with value '%s'", null, ioResult);
          ioResult = null;
        }
      } else if (ioResult instanceof OIdentifiable) {
        ioResult = ((OIdentifiable) ioResult).getRecord();
      } else if (ioResult instanceof Collection<?> || ioResult instanceof OMultiCollectionIterator<?>
          || ioResult.getClass().isArray()) {
        final List<Object> result = new ArrayList<Object>(OMultiValue.getSize(ioResult));
        for (Object o : OMultiValue.getMultiValueIterable(ioResult)) {
          Object newlyAdded = ODocumentHelper.getFieldValue(o, paramAsString);
          if (OMultiValue.isMultiValue(newlyAdded)) {
            for (Object item : OMultiValue.getMultiValueIterable(newlyAdded)) {
              result.add(item);
            }
          } else {
            result.add(newlyAdded);
          }
        }
        return result;
      }
    }

    if (!"*".equals(paramAsString) && ioResult != null) {
      if (ioResult instanceof OCommandContext) {
        ioResult = ((OCommandContext) ioResult).getVariable(paramAsString);
      } else {
        ioResult = ODocumentHelper.getFieldValue(ioResult, paramAsString, iContext);
      }
    }

    return ioResult;
  }

  @Override
  public boolean evaluateParameters() {
    return false;
  }
}
