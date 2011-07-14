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
package com.orientechnologies.orient.core.sql.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.command.OCommandToParse;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;

/**
 * Parsed query. It's built once a query is parsed.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLFilter extends OCommandToParse {
	protected ODatabaseRecord							database;
	protected List<String>								targetRecords;
	protected Map<String, String>					targetClusters;
	protected Map<OClass, String>					targetClasses;
	protected String											targetIndex;
	protected Set<OProperty>							properties	= new HashSet<OProperty>();
	protected OSQLFilterCondition					rootCondition;
	protected List<String>								recordTransformed;
	private List<OSQLFilterItemParameter>	parameterItems;
	private int														braces;

	public OSQLFilter(final ODatabaseRecord iDatabase, final String iText) {
		try {
			database = iDatabase;
			text = iText.trim();
			textUpperCase = text.toUpperCase();

			if (extractTargets()) {
				// IF WHERE EXISTS EXTRACT CONDITIONS

				final StringBuilder word = new StringBuilder();
				int newPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);
				if (newPos > -1 && word.toString().equals(OCommandExecutorSQLAbstract.KEYWORD_WHERE)) {
					currentPos = newPos;
					rootCondition = extractConditions(null);
				}
			}
		} catch (OQueryParsingException e) {
			if (e.getText() == null)
				// QUERY EXCEPTION BUT WITHOUT TEXT: NEST IT
				throw new OQueryParsingException("Error on parsing query", text, currentPos, e);

			throw e;
		} catch (Throwable t) {
			throw new OQueryParsingException("Error on parsing query", text, currentPos, t);
		}
	}

	public boolean evaluate(final ODatabaseRecord iDatabase, final ORecordSchemaAware<?> iRecord) {
		if (targetClasses != null) {
			final OClass cls = targetClasses.keySet().iterator().next();
			// CHECK IF IT'S PART OF THE REQUESTED CLASS
			if (iRecord.getSchemaClass() == null || !iRecord.getSchemaClass().isSubClassOf(cls))
				// EXCLUDE IT
				return false;
		}

		if (rootCondition == null)
			return true;

		return (Boolean) rootCondition.evaluate(iRecord);
	}

	private boolean extractTargets() {
		jumpWhiteSpaces();

		if (currentPos == -1)
			throw new OQueryParsingException("No query target found", text, 0);

		targetRecords = new ArrayList<String>();

		if (Character.isDigit(text.charAt(currentPos))) {
			// UNIQUE RID
			final StringBuilder word = new StringBuilder();
			currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);

			targetRecords.add(word.toString());

		} else if (text.charAt(currentPos) == OStringSerializerHelper.COLLECTION_BEGIN) {
			// COLLECTION OF RIDS
			currentPos = OStringSerializerHelper.getCollection(text, currentPos, targetRecords);
		} else {
			String subjectName;
			String alias;
			String subjectToMatch;
			int newPos;

			final StringBuilder word = new StringBuilder();
			currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);

			while (currentPos > -1 && (targetClasses == null && targetClusters == null && targetIndex == null)) {
				subjectName = word.toString();

				newPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);
				if (newPos > -1 && word.toString().equals("AS")) {
					currentPos = newPos;

					newPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);
					if (newPos == -1)
						throw new OQueryParsingException("No alias found. Example: SELECT FROM Customer AS c", text, currentPos);

					currentPos = newPos;

					alias = word.toString();

				} else
					alias = subjectName;

				subjectToMatch = subjectName;
				if (subjectToMatch.startsWith(OCommandExecutorSQLAbstract.CLUSTER_PREFIX)) {
					// REGISTER AS CLUSTER
					if (targetClusters == null)
						targetClusters = new HashMap<String, String>();
					targetClusters.put(subjectName.substring(OCommandExecutorSQLAbstract.CLUSTER_PREFIX.length()), alias);

				} else if (subjectToMatch.startsWith(OCommandExecutorSQLAbstract.INDEX_PREFIX)) {
					// REGISTER AS INDEX
					targetIndex = subjectName.substring(OCommandExecutorSQLAbstract.INDEX_PREFIX.length());
				} else {
					if (subjectToMatch.startsWith(OCommandExecutorSQLAbstract.CLASS_PREFIX))
						// REGISTER AS CLASS
						subjectName = subjectName.substring(OCommandExecutorSQLAbstract.CLASS_PREFIX.length());

					// REGISTER AS CLASS
					if (targetClasses == null)
						targetClasses = new HashMap<OClass, String>();

					OClass cls = database.getMetadata().getSchema().getClass(subjectName);
					if (cls == null)
						throw new OCommandExecutionException("Class '" + subjectName + "' was not found in current database");

					targetClasses.put(cls, alias);
				}
			}
		}

		return currentPos > -1;
	}

	private OSQLFilterCondition extractConditions(final OSQLFilterCondition iParentCondition) {
		final OSQLFilterCondition currentCondition = extractCondition();

		// CHECK IF THERE IS ANOTHER CONDITION ON RIGHT
		if (!jumpWhiteSpaces())
			// END OF TEXT
			return currentCondition;

		if (currentPos > -1 && text.charAt(currentPos) == ')')
			return currentCondition;

		final OQueryOperator nextOperator = extractConditionOperator();

		if (nextOperator.precedence > currentCondition.getOperator().precedence) {
			// SWAP ITEMS
			final OSQLFilterCondition subCondition = new OSQLFilterCondition(currentCondition.right, nextOperator);
			currentCondition.right = subCondition;
			subCondition.right = extractConditions(subCondition);
			return currentCondition;
		} else {
			final OSQLFilterCondition parentCondition = new OSQLFilterCondition(currentCondition, nextOperator);
			parentCondition.right = extractConditions(parentCondition);
			return parentCondition;
		}
	}

	protected OSQLFilterCondition extractCondition() {
		if (!jumpWhiteSpaces())
			// END OF TEXT
			return null;

		// EXTRACT ITEMS
		final Object left = extractConditionItem(1);
		final OQueryOperator oper = extractConditionOperator();
		final Object right = oper != null ? extractConditionItem(oper.expectedRightWords) : null;

		// CREATE THE CONDITION OBJECT
		return new OSQLFilterCondition(left, oper, right);
	}

	private OQueryOperator extractConditionOperator() {
		if (!jumpWhiteSpaces())
			// END OF PARSING: JUST RETURN
			return null;

		if (text.charAt(currentPos) == ')')
			// FOUND ')': JUST RETURN
			return null;

		String word;
		word = nextWord(true, " 0123456789'\"");

		for (OQueryOperator op : OSQLEngine.getInstance().getRecordOperators()) {
			if (word.startsWith(op.keyword)) {
				final List<String> params = new ArrayList<String>();
				// CHECK FOR PARAMETERS
				if (word.length() > op.keyword.length() && word.charAt(op.keyword.length()) == OStringSerializerHelper.PARENTHESIS_BEGIN) {
					int paramBeginPos = currentPos - (word.length() - op.keyword.length());
					currentPos = OStringSerializerHelper.getParameters(text, paramBeginPos, params);
				} else if (!word.equals(op.keyword))
					throw new OQueryParsingException("Malformed usage of operator '" + op.toString() + "'. Parsed operator is: " + word);

				try {
					return op.configure(params);
				} catch (Exception e) {
					throw new OQueryParsingException("Syntax error using the operator '" + op.toString() + "'. Syntax is: " + op.getSyntax());
				}
			}
		}

		throw new OQueryParsingException("Unknown operator " + word, text, currentPos);
	}

	private Object extractConditionItem(final int iExpectedWords) {
		final Object[] result = new Object[iExpectedWords];

		for (int i = 0; i < iExpectedWords; ++i) {
			String[] words = nextValue(true);
			if (words == null)
				break;

			if (words[0].length() > 0 && words[0].charAt(0) == OStringSerializerHelper.PARENTHESIS_BEGIN) {
				braces++;

				// SUB-CONDITION
				currentPos = currentPos - words[0].length() + 1;

				OSQLFilterCondition subCondition = extractConditions(null);

				if (!jumpWhiteSpaces() || text.charAt(currentPos) == ')')
					braces--;
				currentPos++;

				result[i] = subCondition;
			} else if (words[0].charAt(0) == OStringSerializerHelper.COLLECTION_BEGIN) {
				// COLLECTION OF ELEMENTS
				currentPos = currentPos - words[0].length();

				final List<String> stringItems = new ArrayList<String>();
				currentPos = OStringSerializerHelper.getCollection(text, currentPos, stringItems);

				final List<Object> coll = new ArrayList<Object>();
				for (String s : stringItems) {
					coll.add(OSQLHelper.parseValue(this, database, this, s));
				}

				currentPos++;

				result[i] = coll;

			} else if (words[0].startsWith(OSQLFilterItemFieldAll.NAME + OStringSerializerHelper.PARENTHESIS_BEGIN)) {

				result[i] = new OSQLFilterItemFieldAll(this, words[1]);

			} else if (words[0].startsWith(OSQLFilterItemFieldAny.NAME + OStringSerializerHelper.PARENTHESIS_BEGIN)) {

				result[i] = new OSQLFilterItemFieldAny(this, words[1]);

			} else {

				if (words[0].equals("NOT")) {
					// GET THE NEXT VALUE
					String[] nextWord = nextValue(true);
					if (nextWord != null && nextWord.length == 2) {
						words[1] = words[1] + " " + nextWord[1];

						if (words[1].endsWith(")"))
							words[1] = words[1].substring(0, words[1].length() - 1);
					}
				}

				result[i] = OSQLHelper.parseValue(this, database, this, words[1]);
			}
		}

		return iExpectedWords == 1 ? result[0] : result;
	}

	public Map<String, String> getTargetClusters() {
		return targetClusters;
	}

	public Map<OClass, String> getTargetClasses() {
		return targetClasses;
	}

	public List<String> getTargetRecords() {
		return targetRecords;
	}

	public String getTargetIndex() {
		return targetIndex;
	}

	public OSQLFilterCondition getRootCondition() {
		return rootCondition;
	}

	private String[] nextValue(final boolean iAdvanceWhenNotFound) {
		if (!jumpWhiteSpaces())
			return null;

		int begin = currentPos;
		char c;
		char stringBeginCharacter = ' ';
		int openBraces = 0;
		int openBraket = 0;
		boolean escaped = false;
		boolean escapingOn = false;

		for (; currentPos < text.length(); ++currentPos) {
			c = text.charAt(currentPos);

			if (stringBeginCharacter == ' ' && (c == '"' || c == '\'')) {
				// QUOTED STRING: GET UNTIL THE END OF QUOTING
				stringBeginCharacter = c;
			} else if (stringBeginCharacter != ' ') {
				// INSIDE TEXT
				if (c == '\\') {
					escapingOn = true;
					escaped = true;
				} else {
					if (c == stringBeginCharacter && !escapingOn) {
						stringBeginCharacter = ' ';

						if (openBraket == 0 && openBraces == 0) {
							if (iAdvanceWhenNotFound)
								currentPos++;
							break;
						}
					}

					if (escapingOn)
						escapingOn = false;
				}
			} else if (c == '#' && currentPos == begin) {
				// BEGIN OF RID
			} else if (c == '(') {
				openBraces++;
			} else if (c == ')') {
				openBraces--;
			} else if (c == OStringSerializerHelper.COLLECTION_BEGIN) {
				openBraket++;
			} else if (c == OStringSerializerHelper.COLLECTION_END) {
				openBraket--;
				if (openBraket == 0 && openBraces == 0) {
					currentPos++;
					break;
				}
			} else if (c == ' ' && openBraces == 0) {
				break;
			} else if (!Character.isLetter(c) && !Character.isDigit(c) && c != '.' && c != ':' && c != '-' && c != '_' && c != '+'
					&& c != '@' && openBraces == 0 && openBraket == 0) {
				if (iAdvanceWhenNotFound)
					currentPos++;
				break;
			}
		}

		if (escaped)
			return new String[] { OStringSerializerHelper.decode(textUpperCase.substring(begin, currentPos)),
					OStringSerializerHelper.decode(text.substring(begin, currentPos)) };
		else
			return new String[] { textUpperCase.substring(begin, currentPos), text.substring(begin, currentPos) };
	}

	private String nextWord(final boolean iForceUpperCase, final String iSeparators) {
		StringBuilder word = new StringBuilder();
		currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, iForceUpperCase, iSeparators);
		return word.toString();
	}

	private boolean jumpWhiteSpaces() {
		currentPos = OStringParser.jumpWhiteSpaces(text, currentPos);
		return currentPos > -1;
	}

	@Override
	public String toString() {
		if (rootCondition != null)
			return "Parsed: " + rootCondition.toString();
		return "Unparsed: " + text;
	}

	/**
	 * Binds parameters.
	 * 
	 * @param iArgs
	 */
	public void bindParameters(final Map<Object, Object> iArgs) {
		if (parameterItems == null || iArgs == null || iArgs.size() == 0)
			return;

		if (iArgs.size() < parameterItems.size())
			throw new OCommandExecutionException("Can't execute because " + (parameterItems.size() - iArgs.size())
					+ " parameter(s) are unbounded");

		String paramName;
		for (Entry<Object, Object> entry : iArgs.entrySet()) {
			if (entry.getKey() instanceof Integer)
				parameterItems.get(((Integer) entry.getKey())).setValue(entry.setValue(entry.getValue()));
			else {
				paramName = entry.getKey().toString();
				for (OSQLFilterItemParameter value : parameterItems) {
					if (value.getName().equalsIgnoreCase(paramName)) {
						value.setValue(entry.getValue());
						break;
					}
				}
			}
		}
	}

	public OSQLFilterItemParameter addParameter(final String iName) {
		final String name;
		if (iName.charAt(0) == OStringSerializerHelper.PARAMETER_NAMED) {
			name = iName.substring(1);

			// CHECK THE PARAMETER NAME IS CORRECT
			if (!OStringSerializerHelper.isAlphanumeric(name)) {
				throw new OQueryParsingException("Parameter name '" + name + "' is invalid, only alphanumeric characters are allowed");
			}
		} else
			name = iName;

		final OSQLFilterItemParameter param = new OSQLFilterItemParameter(name);

		if (parameterItems == null)
			parameterItems = new ArrayList<OSQLFilterItemParameter>();

		parameterItems.add(param);
		return param;
	}

	public void setRootCondition(final OSQLFilterCondition iCondition) {
		rootCondition = iCondition;
	}
}
