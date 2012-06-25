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

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

/**
 * @author luca.molino
 * 
 */
public abstract class OCommandExecutorSQLSetAware extends OCommandExecutorSQLAbstract {

  protected static final String KEYWORD_SET      = "SET";

  protected int                 parameterCounter = 0;

  protected int parseSetFields(final Map<String, Object> fields) {
    String fieldName;
    String fieldValue;
    int newPos = currentPos;

    while (currentPos != -1 && (fields.size() == 0 || tempParseWord.toString().equals(","))) {
      newPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, tempParseWord, false);
      if (newPos == -1)
        throw new OCommandSQLParsingException("Field name expected", text, currentPos);
      currentPos = newPos;

      fieldName = tempParseWord.toString();

      newPos = OStringParser.jumpWhiteSpaces(text, currentPos);

      if (newPos == -1 || text.charAt(newPos) != '=')
        throw new OCommandSQLParsingException("Character '=' was expected", text, currentPos);

      currentPos = newPos + 1;
      newPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, tempParseWord, false, " =><");
      if (currentPos == -1)
        throw new OCommandSQLParsingException("Value expected", text, currentPos);

      fieldValue = tempParseWord.toString();

      if (fieldValue.startsWith("{") || fieldValue.startsWith("[") || fieldValue.startsWith("[")) {
        newPos = OStringParser.jumpWhiteSpaces(text, currentPos);
        final StringBuilder buffer = new StringBuilder();
        newPos = OStringSerializerHelper.parse(text, buffer, newPos, -1, OStringSerializerHelper.DEFAULT_FIELD_SEPARATOR, true,
            OStringSerializerHelper.DEFAULT_IGNORE_CHARS);
        fieldValue = buffer.toString();
      }

      if (fieldValue.endsWith(",")) {
        currentPos = newPos - 1;
        fieldValue = fieldValue.substring(0, fieldValue.length() - 1);
      } else
        currentPos = newPos;

      // INSERT TRANSFORMED FIELD VALUE
      fields.put(fieldName, getFieldValueCountingParameters(fieldValue));

      currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, tempParseWord, true);
    }

    if (fields.size() == 0)
      throw new OCommandSQLParsingException("Entries to set <field> = <value> are missed. Example: name = 'Bill', salary = 300.2",
          text, currentPos);

    return currentPos;
  }

  protected Object getFieldValueCountingParameters(String fieldValue) {
    if (fieldValue.trim().equals("?"))
      parameterCounter++;
    return OSQLHelper.parseValue(this, fieldValue, context);
  }

}
