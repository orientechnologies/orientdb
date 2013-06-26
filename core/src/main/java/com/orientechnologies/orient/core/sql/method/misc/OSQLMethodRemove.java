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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Remove elements from a collection.
 * 
 * @author Luca Garulli
 */
public class OSQLMethodRemove extends OAbstractSQLMethod {

  public static final String NAME = "remove";

  public OSQLMethodRemove() {
    super(NAME, 1, -1);
  }

  @Override
  public Object execute(final OIdentifiable iCurrentRecord, final OCommandContext iContext, Object ioResult, Object[] iMethodParams) {
    if (iMethodParams != null && iMethodParams.length > 0 && iMethodParams[0] != null)
      iMethodParams = OMultiValue.array(iMethodParams, Object.class, new OCallable<Object, Object>() {

        @Override
        public Object call(final Object iArgument) {
          if (iArgument instanceof String && ((String) iArgument).startsWith("$"))
            return iContext.getVariable((String) iArgument);
          return iArgument;
        }
      });

    for (Object o : iMethodParams) {
      ioResult = OMultiValue.remove(ioResult, o);
    }

    return ioResult;
  }
}
