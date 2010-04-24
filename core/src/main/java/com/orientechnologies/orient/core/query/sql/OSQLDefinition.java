/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.query.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.query.OQueryHelper;
import com.orientechnologies.orient.core.query.operator.OQueryOperator;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;

/**
 * Parsed query. It's built once a query is parsed.
 * 
 * @author luca
 * 
 */
public class OSQLDefinition {
	private static final String		CLASS_PREFIX		= "class:";
	private static final String		CLUSTER_PREFIX	= "cluster:";
	protected OSQLQuery<?>				query;
	protected String							text;
	protected String							textUpperCase;
	protected int									currentPos			= 0;
	protected List<String>				projections			= new ArrayList<String>();
	protected Map<String, String>	clusters				= new HashMap<String, String>();
	protected Map<String, String>	classes					= new HashMap<String, String>();
	protected OSQLDefinitionCondition				rootCondition;
	protected List<String>				recordTransformed;
	private int										braces;

	protected static final String	KEYWORD_SELECT	= "SELECT";
	protected static final String	KEYWORD_FROM		= "FROM";
	protected static final String	KEYWORD_WHERE		= "WHERE";
	protected static final String	KEYWORD_COLUMN	= "COLUMN";

	public OSQLDefinition(OSQLQuery<?> iQuery, String iText) {
		try {
			query = iQuery;
			text = iText.trim();
			textUpperCase = text.toUpperCase();

			extractProjections();
			if (extractClustersAndClasses()) {
				// IF WHERE EXISTS EXTRACT CONDITIONS
				rootCondition = extractConditions(null);
			}
		} catch (OQueryParsingException e) {
			throw e;
		} catch (Throwable t) {
			throw new OQueryParsingException("", text, currentPos);
		}
	}

	public boolean evaluate(ODatabaseRecord<?> iDatabase, ORecordSchemaAware<?> iRecord) {
		if (rootCondition == null)
			return true;
		return (Boolean) rootCondition.evaluate(iRecord);
	}

	private void extractProjections() {
		String word = nextWord(true);
		if (!word.equals(KEYWORD_SELECT))
			return;

		int fromPosition = textUpperCase.indexOf(KEYWORD_FROM, currentPos);
		if (fromPosition == -1)
			throw new OQueryParsingException("Missed " + KEYWORD_FROM, text, currentPos);

		String[] items = textUpperCase.substring(currentPos, fromPosition).split(",");
		if (items == null || items.length == 0)
			throw new OQueryParsingException("No projections found between " + KEYWORD_SELECT + " and " + KEYWORD_FROM, text, currentPos);

		for (String i : items)
			projections.add(i.trim());

		currentPos = fromPosition + KEYWORD_FROM.length() + 1;
	}

	private boolean extractClustersAndClasses() {
		jumpWhiteSpaces();

		boolean whereDefined = true;

		int wherePosition = textUpperCase.indexOf(KEYWORD_WHERE, currentPos);
		if (wherePosition == -1) {
			// NO WHERE CONDITION: GET UNTIL THE END AND ASSURE TO RETURN FALSE IN ORDER TO AVOID PARSING OF CONDITIONS
			whereDefined = false;
			wherePosition = text.length();
		}

		String[] items = textUpperCase.substring(currentPos, wherePosition).split(",");
		if (items == null || items.length == 0)
			throw new OQueryParsingException("No clusters found after " + KEYWORD_FROM, text, currentPos);

		String[] words;
		String subjectName;
		String alias;
		String subjectToMatch;
		for (String i : items) {
			words = i.split(" ");

			if (words != null && words.length > 1) {
				// FOUND ALIAS
				subjectName = words[0].trim();
				alias = words[1].trim();
			} else {
				subjectName = i.trim();
				alias = subjectName;
			}

			subjectToMatch = subjectName.toLowerCase();
			if (subjectToMatch.startsWith(CLASS_PREFIX))
				// REGISTER AS CLASS
				classes.put(subjectName.substring(CLASS_PREFIX.length()), alias);
			else if (subjectToMatch.startsWith(CLUSTER_PREFIX))
				// REGISTER AS CLUSTER
				clusters.put(subjectName.substring(CLUSTER_PREFIX.length()), alias);
			else
				// DEFAULT: REGISTER AS CLUSTER
				clusters.put(subjectName, alias);
		}

		if (whereDefined)
			currentPos = wherePosition + KEYWORD_WHERE.length() + 1;

		return whereDefined;
	}

	private OSQLDefinitionCondition extractConditions(OSQLDefinitionCondition iParentCondition) {
		OSQLDefinitionCondition currentCondition = extractCondition();

		// CHECK IF THERE IS ANOTHER CONDITION ON RIGHT
		if (!jumpWhiteSpaces())
			// END OF TEXT
			return currentCondition;

		OQueryOperator nextOperator = extractConditionOperator();

		OSQLDefinitionCondition parentCondition = new OSQLDefinitionCondition(currentCondition, nextOperator);

		parentCondition.right = extractConditions(parentCondition);

		return parentCondition;
	}

	protected OSQLDefinitionCondition extractCondition() {
		if (!jumpWhiteSpaces())
			// END OF TEXT
			return null;

		braces = 0;

		// CREATE THE CONDITION OBJECT
		return new OSQLDefinitionCondition(extractConditionItem(), extractConditionOperator(), extractConditionItem());
	}

	private OQueryOperator extractConditionOperator() {
		String word;
		word = nextWord(true);

		for (OQueryOperator op : OSQLQuery.getOperators()) {
			if (word.startsWith(op.keyword)) {
				String[] params = null;

				// CHECK FOR PARAMETERS
				if (word.endsWith(OQueryHelper.OPEN_BRACE)) {
					params = OQueryHelper.getParameters(text, currentPos - 1);
					currentPos = text.indexOf(OQueryHelper.CLOSED_BRACE, currentPos) + 1;
				}

				return op.configure(params);
			}
		}

		throw new OQueryParsingException("Unknown operator " + word, text, currentPos);
	}

	private Object extractConditionItem() {
		Object result = null;
		String[] words = nextValue(true);
		if (words == null)
			return null;

		if (words[0].startsWith(OQueryHelper.OPEN_BRACE)) {
			braces++;

			// SUB-CONDITION
			currentPos = currentPos - words[0].length() + 1;

			OSQLDefinitionCondition subCondition = new OSQLDefinitionCondition(extractConditionItem(), extractConditionOperator(), extractConditionItem());

			jumpWhiteSpaces();

			currentPos++;

			braces--;

			return subCondition;
		} else if (words[0].startsWith(OQueryHelper.OPEN_COLLECTION)) {
			// COLLECTION OF ELEMENTS
			currentPos = currentPos - words[0].length() + 1;

			List<Object> coll = new ArrayList<Object>();

			String[] item;
			Object v;
			do {
				item = nextValue(true);

				v = getValue(item);
				coll.add(v);

				item = nextValue(true);
			} while (item != null && item[0].equals(OQueryHelper.COLLECTION_SEPARATOR));

			currentPos++;

			return coll;
		} else if (words[0].startsWith(KEYWORD_COLUMN)) {

			String[] parameters = OQueryHelper.getParameters(words[0]);
			if (parameters.length != 1)
				throw new OQueryParsingException("Missed column number", text, currentPos);
			result = new OSQLDefinitionItemColumn(this, parameters[0]);

		} else if (words[0].startsWith(OSQLDefinitionItemFieldAll.NAME + OQueryHelper.OPEN_BRACE)) {

			String[] parameters = OQueryHelper.getParameters(words[0]);

			// IF THE FIELD IS ONLY ONE, THEN USE THE FIELD CLASS IN PLACE OF MULTI FIELD IMPL
			result = parameters.length == 1 ? getValue(words) : new OSQLDefinitionItemFieldAll(this, parameters);

		} else if (words[0].startsWith(OSQLDefinitionItemFieldAny.NAME + OQueryHelper.OPEN_BRACE)) {

			String[] parameters = OQueryHelper.getParameters(words[0]);

			// IF THE FIELD IS ONLY ONE, THEN USE THE FIELD CLASS IN PLACE OF MULTI FIELD IMPL
			result = parameters.length == 1 ? getValue(words) : new OSQLDefinitionItemFieldAny(this, parameters);

		} else
			result = getValue(words);

		return result;
	}

	private Object getValue(final String[] words) {
		if (words[0].startsWith("'") || words[0].startsWith("\""))
			return stringContent(words[1]);
		else if (Character.isDigit(words[0].charAt(0)) || words[0].charAt(0) == '+' || words[0].charAt(0) == '-') {
			if (words[0].contains("."))
				return new Float(words[0]);
			else
				return new Integer(words[0]);
		}

		return new OSQLDefinitionItemField(this, words[1]);
	}

	public List<String> getProjections() {
		return projections;
	}

	public Map<String, String> getClusters() {
		return clusters;
	}

	public Map<String, String> getClasses() {
		return classes;
	}

	public Object getRootCondition() {
		return rootCondition;
	}

	private boolean jumpWhiteSpaces() {
		for (; currentPos < text.length(); ++currentPos)
			if (!Character.isWhitespace(text.charAt(currentPos)))
				break;
		return currentPos < text.length();
	}

	private String[] nextValue(final boolean iAdvanceWhenNotFound) {
		if (!jumpWhiteSpaces())
			return null;

		int begin = currentPos;
		char c;
		char stringBeginCharacter = ' ';
		int openBraces = 0;
		int openBraket = 0;

		for (; currentPos < text.length(); ++currentPos) {
			c = text.charAt(currentPos);

			if (stringBeginCharacter == ' ' && (c == '"' || c == '\'')) {
				// QUOTED STRING: GET UNTIL THE END OF QUOTING
				stringBeginCharacter = c;
			} else if (stringBeginCharacter != ' ') {
				if (c == stringBeginCharacter) {
					stringBeginCharacter = ' ';

					if (openBraket == 0 && openBraces == 0) {
						if (iAdvanceWhenNotFound)
							currentPos++;
						break;
					}
				}
			} else if (c == '(') {
				openBraces++;
			} else if (c == ')') {
				openBraces--;
			} else if (c == '[') {
				openBraket++;
			} else if (c == ']') {
				openBraket--;
				if (openBraket == 0 && openBraces == 0) {
					currentPos++;
					break;
				}
			} else if (c == ' ' && openBraces == 0) {
				break;
			} else if (!Character.isLetter(c) && !Character.isDigit(c) && c != '.' && c != '-' && c != '+' && openBraces == 0
					&& openBraket == 0) {
				if (iAdvanceWhenNotFound)
					currentPos++;
				break;
			}
		}

		return new String[] { textUpperCase.substring(begin, currentPos), text.substring(begin, currentPos) };
	}

	private String nextWord(boolean iForceUpperCase) {
		if (!jumpWhiteSpaces())
			return null;

		int begin = currentPos;

		for (; currentPos < text.length(); ++currentPos)
			if (Character.isWhitespace(text.charAt(currentPos)))
				break;

		return iForceUpperCase ? textUpperCase.substring(begin, currentPos) : text.substring(begin, currentPos);
	}

	protected String stringContent(String iContent) {
		return iContent.substring(1, iContent.length() - 1);
	}

	@Override
	public String toString() {
		if (rootCondition != null)
			return "Parsed: " + rootCondition.toString();
		return "Unparsed: " + text;
	}
}
