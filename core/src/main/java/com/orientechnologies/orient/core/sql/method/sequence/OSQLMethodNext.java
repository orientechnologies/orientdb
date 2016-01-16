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
package com.orientechnologies.orient.core.sql.method.sequence;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.method.misc.OAbstractSQLMethod;

/**
 * Returns the next number of a sequence.
 * 
 * @author Luca Garulli
 */
public class OSQLMethodNext extends OAbstractSQLMethod {

  public static final String NAME = "next";

  public OSQLMethodNext() {
    super(NAME, 0, 0);
  }

  @Override
  public String getSyntax() {
    return "next()";
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, OCommandContext iContext, Object ioResult, Object[] iParams) {
    if (iThis == null)
      throw new OCommandSQLParsingException("Method 'next()' can be invoked only on OSequence instances, while NULL was found");

    if (!(iThis instanceof OSequence))
      throw new OCommandSQLParsingException("Method 'next()' can be invoked only on OSequence instances, while '"
          + iThis.getClass() + "' was found");

    return ((OSequence) iThis).next();
  }
}
