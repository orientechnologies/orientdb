/*
 *
 * Copyright 2012 Luca Molino (molino.luca--AT--gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql;

import java.util.Map;

import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

/**
 * @author luca.molino
 * 
 */
public abstract class OCommandExecutorSQLSetAware extends OCommandExecutorSQLAbstract {

  protected static final String KEYWORD_SET      = "SET";

  protected int                 parameterCounter = 0;

  protected void parseSetFields(final Map<String, Object> fields) {
    String fieldName;
    String fieldValue;

    while (!parserIsEnded() && (fields.size() == 0 || parserGetLastSeparator() == ',')) {
      fieldName = parserRequiredWord(false, "Field name expected");
      parserRequiredKeyword("=");
      fieldValue = parserRequiredWord(false, "Value expected", " =><,\r\n");

      if (fieldValue.startsWith("{") || fieldValue.startsWith("[") || fieldValue.startsWith("[")) {
        final StringBuilder buffer = new StringBuilder();
        parserSetCurrentPosition(OStringSerializerHelper.parse(text, buffer, parserGetPreviousPosition(), -1,
            OStringSerializerHelper.DEFAULT_FIELD_SEPARATOR, true, OStringSerializerHelper.DEFAULT_IGNORE_CHARS));
        fieldValue = buffer.toString();
        parserSetLastSeparator(' ');
      }

      // INSERT TRANSFORMED FIELD VALUE
      fields.put(fieldName, getFieldValueCountingParameters(fieldValue));
    }

    if (fields.size() == 0)
      throwParsingException("Entries to set <field> = <value> are missed. Example: name = 'Bill', salary = 300.2");
  }

  protected Object getFieldValueCountingParameters(String fieldValue) {
    if (fieldValue.trim().equals("?"))
      parameterCounter++;
    return OSQLHelper.parseValue(this, fieldValue, context);
  }

}
