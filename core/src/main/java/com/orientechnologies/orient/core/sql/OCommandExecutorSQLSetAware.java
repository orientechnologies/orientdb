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

/**
 * @author luca.molino
 * 
 */
public abstract class OCommandExecutorSQLSetAware extends OCommandExecutorSQLAbstract {

	protected static final String	KEYWORD_SET				= "SET";

	protected int									parameterCounter	= 0;

	protected int parseSetFields(final StringBuilder word, int pos, final Map<String, Object> fields) {
		String fieldName;
		String fieldValue;
		int newPos = pos;

		while (pos != -1 && (fields.size() == 0 || word.toString().equals(","))) {
			newPos = OSQLHelper.nextWord(text, textUpperCase, pos, word, false);
			if (newPos == -1)
				throw new OCommandSQLParsingException("Field name expected", text, pos);
			pos = newPos;

			fieldName = word.toString();

			newPos = OStringParser.jumpWhiteSpaces(text, pos);

			if (newPos == -1 || text.charAt(newPos) != '=')
				throw new OCommandSQLParsingException("Character '=' was expected", text, pos);

			pos = newPos;
			newPos = OSQLHelper.nextWord(text, textUpperCase, pos + 1, word, false, " =><");
			if (pos == -1)
				throw new OCommandSQLParsingException("Value expected", text, pos);

			fieldValue = word.toString();

			while (pos > -1 && (fieldValue.startsWith("{") && (!fieldValue.endsWith("}") && !fieldValue.endsWith("},")))
					|| (fieldValue.startsWith("[") && (!fieldValue.endsWith("],") && !fieldValue.endsWith("]")))) {
				pos = newPos;
				newPos = OSQLHelper.nextWord(text, textUpperCase, pos + 1, word, false, " =><");
				fieldValue += word.toString();
			}

			if (fieldValue.endsWith(",")) {
				pos = newPos - 1;
				fieldValue = fieldValue.substring(0, fieldValue.length() - 1);
			} else
				pos = newPos;

			// INSERT TRANSFORMED FIELD VALUE
			fields.put(fieldName, getFieldValueCountingParameters(fieldValue));

			pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		}

		if (fields.size() == 0)
			throw new OCommandSQLParsingException("Entries to set <field> = <value> are missed. Example: name = 'Bill', salary = 300.2",
					text, pos);

		return pos;
	}

	protected Object getFieldValueCountingParameters(String fieldValue) {
		if (fieldValue.trim().equals("?"))
			parameterCounter++;
		return OSQLHelper.parseValue(this, fieldValue, context);
	}

}
