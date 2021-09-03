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
package com.orientechnologies.orient.core.sql.functions.text;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.method.misc.OAbstractSQLMethod;

/**
 * Converts a document in JSON string.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLMethodFromJSON extends OAbstractSQLMethod {

  public static final String NAME = "fromjson";

  public OSQLMethodFromJSON() {
    super(NAME, 0, 1);
  }

  @Override
  public String getSyntax() {
    return "fromJSON([<options>])";
  }

  @Override
  public Object execute(
      Object iThis,
      OIdentifiable iCurrentRecord,
      OCommandContext iContext,
      Object ioResult,
      Object[] iParams) {
    if (iThis instanceof String) {
      if (iParams.length > 0) {
        final ODocument doc = new ODocument().fromJSON(iThis.toString(), iParams[0].toString());
        if (iParams[0].toString().contains("embedded"))
          ODocumentInternal.addOwner(doc, iCurrentRecord.getRecord());

        return doc;
      }

      return new ODocument().fromJSON(iThis.toString().toString());
    }

    return null;
  }
}
