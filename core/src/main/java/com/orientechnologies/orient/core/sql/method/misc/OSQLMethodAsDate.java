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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import java.text.ParseException;
import java.util.Date;

/**
 * 
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLMethodAsDate extends OAbstractSQLMethod {

  public static final String NAME = "asdate";

  public OSQLMethodAsDate() {
    super(NAME);
  }

  @Override
  public Object execute(OIdentifiable iCurrentRecord, OCommandContext iContext, Object ioResult, Object[] iMethodParams)
      throws ParseException {

    if (ioResult != null) {
      if (ioResult instanceof Number) {
        ioResult = new Date(((Number) ioResult).longValue());
      } else if (!(ioResult instanceof Date)) {
        ioResult = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().getDateFormatInstance()
            .parse(ioResult.toString());
      }
    }
    return ioResult;
  }
}
