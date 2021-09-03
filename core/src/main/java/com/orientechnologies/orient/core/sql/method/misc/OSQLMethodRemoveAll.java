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
package com.orientechnologies.orient.core.sql.method.misc;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Remove all the occurrences of elements from a collection.
 *
 * @see OSQLMethodRemove
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLMethodRemoveAll extends OAbstractSQLMethod {

  public static final String NAME = "removeall";

  public OSQLMethodRemoveAll() {
    super(NAME, 1, -1);
  }

  @Override
  public Object execute(
      Object iThis,
      final OIdentifiable iCurrentRecord,
      final OCommandContext iContext,
      Object ioResult,
      Object[] iParams) {
    if (iParams != null && iParams.length > 0 && iParams[0] != null) {
      iParams =
          OMultiValue.array(
              iParams,
              Object.class,
              new OCallable<Object, Object>() {

                @Override
                public Object call(final Object iArgument) {
                  if (iArgument instanceof String && ((String) iArgument).startsWith("$")) {
                    return iContext.getVariable((String) iArgument);
                  }
                  return iArgument;
                }
              });
      for (Object o : iParams) {
        ioResult = OMultiValue.remove(ioResult, o, true);
      }
    }

    return ioResult;
  }
}
