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
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * 
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLMethodSize extends OAbstractSQLMethod {

  public static final String NAME = "size";

  public OSQLMethodSize() {
    super(NAME);
  }

  @Override
  public Object execute(final OIdentifiable iCurrentRecord, final OCommandContext iContext, final Object ioResult,
      final Object[] iMethodParams) {
    
    final Number size;
    if (ioResult != null) {
      if (ioResult instanceof ORecord<?>)
        size = 1;
      else
        size = OMultiValue.getSize(ioResult);
    } else
      size = 0;

    return size;
  }
}
