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
package com.orientechnologies.orient.core.sql.functions.misc;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.method.misc.OAbstractSQLMethod;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Filter the content by including only some fields. If the content is a document, then creates a
 * copy with only the included fields. If it's a collection of documents it acts against on each
 * single entry.
 *
 * <p>
 *
 * <p>Syntax:
 *
 * <blockquote>
 *
 * <p>
 *
 * <pre>
 * include(&lt;field|value|expression&gt; [,&lt;field-name&gt;]* )
 * </pre>
 *
 * <p>
 *
 * </blockquote>
 *
 * <p>
 *
 * <p>Examples:
 *
 * <blockquote>
 *
 * <p>
 *
 * <pre>
 * SELECT <b>include(roles, 'name')</b> FROM OUser
 * </pre>
 *
 * <p>
 *
 * </blockquote>
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLMethodInclude extends OAbstractSQLMethod {

  public static final String NAME = "include";

  public OSQLMethodInclude() {
    super(NAME, 1, -1);
  }

  @Override
  public String getSyntax() {
    return "Syntax error: include([<field-name>][,]*)";
  }

  @Override
  public Object execute(
      Object iThis,
      OIdentifiable iCurrentRecord,
      OCommandContext iContext,
      Object ioResult,
      Object[] iParams) {

    if (iParams[0] != null) {
      if (iThis instanceof OIdentifiable) {
        iThis = ((OIdentifiable) iThis).getRecord();
      }
      if (iThis instanceof ODocument) {
        // ACT ON SINGLE DOCUMENT
        return copy((ODocument) iThis, iParams);
      } else if (iThis instanceof Map) {
        // ACT ON MAP
        return copy((Map) iThis, iParams);
      } else if (OMultiValue.isMultiValue(iThis)) {
        // ACT ON MULTIPLE DOCUMENTS
        final List<Object> result = new ArrayList<Object>(OMultiValue.getSize(iThis));
        for (Object o : OMultiValue.getMultiValueIterable(iThis, false)) {
          if (o instanceof OIdentifiable) {
            result.add(copy((ODocument) ((OIdentifiable) o).getRecord(), iParams));
          }
        }
        return result;
      }
    }

    // INVALID, RETURN NULL
    return null;
  }

  private Object copy(final ODocument document, final Object[] iFieldNames) {
    final ODocument doc = new ODocument();
    for (int i = 0; i < iFieldNames.length; ++i) {
      if (iFieldNames[i] != null) {

        final String fieldName = (String) iFieldNames[i].toString();

        if (fieldName.endsWith("*")) {
          final String fieldPart = fieldName.substring(0, fieldName.length() - 1);
          final List<String> toInclude = new ArrayList<String>();
          for (String f : document.fieldNames()) {
            if (f.startsWith(fieldPart)) toInclude.add(f);
          }

          for (String f : toInclude) doc.field(fieldName, document.<Object>field(f));

        } else doc.field(fieldName, document.<Object>field(fieldName));
      }
    }
    return doc;
  }

  private Object copy(final Map map, final Object[] iFieldNames) {
    final ODocument doc = new ODocument();
    for (int i = 0; i < iFieldNames.length; ++i) {
      if (iFieldNames[i] != null) {
        final String fieldName = iFieldNames[i].toString();

        if (fieldName.endsWith("*")) {
          final String fieldPart = fieldName.substring(0, fieldName.length() - 1);
          final List<String> toInclude = new ArrayList<String>();
          for (Object f : map.keySet()) {
            if (f.toString().startsWith(fieldPart)) toInclude.add(f.toString());
          }

          for (String f : toInclude) doc.field(fieldName, map.get(f));

        } else doc.field(fieldName, map.get(fieldName));
      }
    }
    return doc;
  }
}
