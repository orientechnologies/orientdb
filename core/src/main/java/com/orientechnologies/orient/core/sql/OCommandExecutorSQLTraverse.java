/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

/**
 * Executes a TRAVERSE crossing records. Returns a List<OIdentifiable> containing all the traversed records that match the WHERE
 * condition.
 * <p>
 * SYNTAX: <code>TRAVERSE <field>* FROM <target> WHERE <condition></code>
 * </p>
 * <p>
 * In the command context you've access to the variable $depth containing the depth level from the root node. This is useful to
 * limit the traverse up to a level. For example to consider from the first depth level (0 is root node) to the third use:
 * <code>TRAVERSE children FROM #5:23 WHERE $depth BETWEEN 1 AND 3</code>. To filter traversed records use it combined with a SELECT
 * statement:
 * </p>
 * <p>
 * <code>SELECT FROM (TRAVERSE children FROM #5:23 WHERE $depth BETWEEN 1 AND 3) WHERE city.name = 'Rome'</code>
 * </p>
 * 
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLTraverse extends OCommandExecutorSQLExtractAbstract {
	public static final String	KEYWORD_TRAVERSE	= "TRAVERSE";

	private Set<String>					fields;

	/**
	 * Compile the filter conditions only the first time.
	 */
	public OCommandExecutorSQLTraverse parse(final OCommandRequestText iRequest) {
		super.parse(iRequest);

		final int pos = parseFields();
		if (pos == -1)
			return this;

		int endPosition = text.length();
		int endP = textUpperCase.indexOf(" " + OCommandExecutorSQLTraverse.KEYWORD_LIMIT, currentPos);
		if (endP > -1 && endP < endPosition)
			endPosition = endP;

		compiledFilter = OSQLEngine.getInstance().parseFromWhereCondition(text.substring(pos, endPosition));

		optimize();

		currentPos = compiledFilter.currentPos < 0 ? endPosition : compiledFilter.currentPos + pos;

		if (currentPos > -1 && currentPos < text.length()) {
			currentPos = OStringParser.jump(text, currentPos, " \r\n");

			final StringBuilder word = new StringBuilder();
			String w;

			while (currentPos > -1) {
				currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);

				if (currentPos > -1) {
					w = word.toString();
					if (w.equals(KEYWORD_LIMIT))
						parseLimit(word);
				}
			}
		}
		if (limit == 0 || limit < -1) {
			throw new IllegalArgumentException("Limit must be > 0 or = -1 (no limit)");
		}
		return this;
	}

	public Object execute(final Map<Object, Object> iArgs) {
		if (!assignTarget(iArgs))
			throw new OQueryParsingException("No source found in query: specify class, cluster(s) or single record(s)");

		executeSearch();

		applyLimit();

		return handleResult();
	}

	protected int parseFields() {
		int currentPos = 0;
		final StringBuilder word = new StringBuilder();

		currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);
		if (!word.toString().equals(KEYWORD_TRAVERSE))
			return -1;

		int fromPosition = textUpperCase.indexOf(KEYWORD_FROM_2FIND, currentPos);
		if (fromPosition == -1)
			throw new OQueryParsingException("Missed " + KEYWORD_FROM, text, currentPos);

		final String fieldString = text.substring(currentPos, fromPosition).trim();
		if (fieldString.length() > 0) {
			// EXTRACT PROJECTIONS
			fields = new HashSet<String>();
			final List<String> items = OStringSerializerHelper.smartSplit(fieldString, ',');

			for (String field : items)
				fields.add(field.trim());
		} else
			throw new OQueryParsingException(
					"Missed field list to cross in TRAVERSE. Use TRAVERSE <field> FROM <target> [WHERE <filter>]", text, currentPos);

		currentPos = fromPosition + KEYWORD_FROM.length() + 1;

		return currentPos;
	}
}
