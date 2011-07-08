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

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandToParse;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordFactory;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

/**
 * Represents an object field as value in the query condition.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OSQLFilterItemAbstract implements OSQLFilterItem {
	protected String															name;
	protected List<OPair<Integer, List<String>>>	operationsChain	= null;

	public OSQLFilterItemAbstract(final OCommandToParse iQueryToParse, final String iName) {
		int separatorPos = iName.indexOf(OSQLFilterFieldOperator.CHAIN_SEPARATOR);
		if (separatorPos > -1) {
			// GET ALL SPECIAL OPERATIONS
			name = iName.substring(0, separatorPos);

			String part = iName;
			String partUpperCase = part.toUpperCase();
			boolean operatorFound;

			while (separatorPos > -1) {
				part = part.substring(separatorPos + OSQLFilterFieldOperator.CHAIN_SEPARATOR.length());
				partUpperCase = partUpperCase.substring(separatorPos + OSQLFilterFieldOperator.CHAIN_SEPARATOR.length());

				operatorFound = false;
				for (OSQLFilterFieldOperator op : OSQLFilterFieldOperator.OPERATORS)
					if (partUpperCase.startsWith(op.keyword + "(")) {
						// OPERATOR MATCH
						final List<String> arguments;

						if (op.minArguments > 0) {
							arguments = OStringSerializerHelper.getParameters(part);
							if (arguments.size() < op.minArguments || arguments.size() > op.maxArguments)
								throw new OQueryParsingException(iQueryToParse.text, "Syntax error: field operator '" + op.keyword + "' needs "
										+ (op.minArguments == op.maxArguments ? op.minArguments : op.minArguments + "-" + op.maxArguments)
										+ " argument(s) while has been received " + arguments.size(), iQueryToParse.currentPos + separatorPos);
						} else
							arguments = null;

						// SPECIAL OPERATION FOUND: ADD IT IN TO THE CHAIN
						if (operationsChain == null)
							operationsChain = new ArrayList<OPair<Integer, List<String>>>();

						operationsChain.add(new OPair<Integer, List<String>>(op.id, arguments));

						separatorPos = partUpperCase.indexOf(OStringSerializerHelper.PARENTHESIS_END)
								+ OSQLFilterFieldOperator.CHAIN_SEPARATOR.length();
						operatorFound = true;
						break;
					}

				if (!operatorFound) {
					separatorPos = partUpperCase.indexOf(OSQLFilterFieldOperator.CHAIN_SEPARATOR, 0);

					// CHECK IF IT'S A FIELD
					int posOpenBrace = part.indexOf('(');
					if (posOpenBrace == -1 || posOpenBrace > separatorPos && separatorPos > -1) {
						// YES, SEEMS A FIELD
						String chainedFieldName = separatorPos > -1 ? part.substring(0, separatorPos) : part;

						if (operationsChain == null)
							operationsChain = new ArrayList<OPair<Integer, List<String>>>();

						final List<String> list = new ArrayList<String>();
						list.add(chainedFieldName);
						if (chainedFieldName.charAt(0) == '@')
							operationsChain.add(new OPair<Integer, List<String>>(OSQLFilterFieldOperator.ATTRIB.id, list));
						else
							operationsChain.add(new OPair<Integer, List<String>>(OSQLFilterFieldOperator.FIELD.id, list));
					} else
						// ERROR: OPERATOR NOT FOUND OR MISPELLED
						throw new OQueryParsingException(iQueryToParse.text,
								"Syntax error: field operator not recognized between the supported ones: "
										+ Arrays.toString(OSQLFilterFieldOperator.OPERATORS), iQueryToParse.currentPos + separatorPos);
				}

				if (separatorPos >= partUpperCase.length())
					return;

				separatorPos = partUpperCase.indexOf(OSQLFilterFieldOperator.CHAIN_SEPARATOR, separatorPos);
			}
		} else
			name = iName;
	}

	public Object transformValue(final ODatabaseRecord iDatabase, Object ioResult) {
		if (ioResult != null && operationsChain != null) {
			// APPLY OPERATIONS FOLLOWING THE STACK ORDER
			int operator = -2;

			try {
				for (OPair<Integer, List<String>> op : operationsChain) {
					operator = op.key.intValue();

					// NO ARGS OPERATORS
					if (operator == OSQLFilterFieldOperator.SIZE.id)
						ioResult = ioResult != null ? OMultiValue.getSize(ioResult) : 0;

					else if (operator == OSQLFilterFieldOperator.LENGTH.id)
						ioResult = ioResult != null ? ioResult.toString().length() : 0;

					else if (operator == OSQLFilterFieldOperator.TOUPPERCASE.id)
						ioResult = ioResult != null ? ioResult.toString().toUpperCase() : 0;

					else if (operator == OSQLFilterFieldOperator.TOLOWERCASE.id)
						ioResult = ioResult != null ? ioResult.toString().toLowerCase() : 0;

					else if (operator == OSQLFilterFieldOperator.TRIM.id)
						ioResult = ioResult != null ? ioResult.toString().trim() : null;

					else if (operator == OSQLFilterFieldOperator.ATTRIB.id) {
						ioResult = getRecordAttribute(iDatabase, (OIdentifiable) ioResult, op.value.get(0));

					} else if (operator == OSQLFilterFieldOperator.FIELD.id) {
						if (ioResult != null) {

							ODocument record;
							if (ioResult instanceof String) {
								try {
									record = new ODocument(iDatabase, new ORecordId((String) ioResult));
								} catch (Exception e) {
									OLogManager.instance().error(this, "Error on reading rid with value '%s'", null, ioResult);
									record = null;
								}
							} else if (ioResult instanceof ORID)
								record = new ODocument(iDatabase, (ORID) ioResult);
							else if (ioResult instanceof ORecord<?>)
								record = (ODocument) ioResult;
							else
								throw new IllegalArgumentException("Field " + name + " is not a ODocument object");

							if (record == null)
								ioResult = null;
							else
								try {
									if (record.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED)
										record.load();

									ioResult = ioResult != null ? record.rawField(op.value.get(0)) : null;
								} catch (ORecordNotFoundException e) {
									ioResult = null;
								}
						}

						// OTHER OPERATORS
					} else if (operator == OSQLFilterFieldOperator.CHARAT.id) {
						int index = Integer.parseInt(op.value.get(0));
						ioResult = ioResult != null ? ioResult.toString().substring(index, index + 1) : null;

					} else if (operator == OSQLFilterFieldOperator.INDEXOF.id && op.value.get(0).length() > 2) {
						String toFind = op.value.get(0).substring(1, op.value.get(0).length() - 1);
						int startIndex = op.value.size() > 1 ? Integer.parseInt(op.value.get(1)) : 0;
						ioResult = ioResult != null ? ioResult.toString().indexOf(toFind, startIndex) : null;

					} else if (operator == OSQLFilterFieldOperator.SUBSTRING.id) {
						int endIndex = op.value.size() > 1 ? Integer.parseInt(op.value.get(1)) : op.value.get(0).length();
						ioResult = ioResult != null ? ioResult.toString().substring(Integer.parseInt(op.value.get(0)), endIndex) : null;

					} else if (operator == OSQLFilterFieldOperator.APPEND.id) {
						String v = op.value.get(0);
						if (v.charAt(0) == '\'' || v.charAt(0) == '"')
							v = v.substring(1, v.length() - 1);
						ioResult = ioResult != null ? ioResult.toString() + v : null;

					} else if (operator == OSQLFilterFieldOperator.PREFIX.id) {
						String v = op.value.get(0);
						if (v.charAt(0) == '\'' || v.charAt(0) == '"')
							v = v.substring(1, v.length() - 1);
						ioResult = ioResult != null ? v + ioResult.toString() : null;

					} else if (operator == OSQLFilterFieldOperator.FORMAT.id)
						ioResult = ioResult != null ? String.format(op.value.get(0), ioResult) : null;

					else if (operator == OSQLFilterFieldOperator.LEFT.id) {
						final int len = Integer.parseInt(op.value.get(0));
						ioResult = ioResult != null ? ioResult.toString().substring(0,
								len <= ioResult.toString().length() ? len : ioResult.toString().length()) : null;

					} else if (operator == OSQLFilterFieldOperator.RIGHT.id) {
						final int offset = Integer.parseInt(op.value.get(0));
						ioResult = ioResult != null ? ioResult.toString().substring(
								offset <= ioResult.toString().length() - 1 ? offset : ioResult.toString().length() - 1) : null;

					} else if (operator == OSQLFilterFieldOperator.ASSTRING.id)
						ioResult = ioResult != null ? ioResult.toString() : null;
					else if (operator == OSQLFilterFieldOperator.ASINTEGER.id)
						ioResult = ioResult != null ? new Integer(ioResult.toString()) : null;
					else if (operator == OSQLFilterFieldOperator.ASFLOAT.id)
						ioResult = ioResult != null ? new Float(ioResult.toString()) : null;
					else if (operator == OSQLFilterFieldOperator.ASBOOLEAN.id) {
						if (ioResult != null) {
							if (ioResult instanceof String)
								ioResult = new Boolean((String) ioResult);
							else if (ioResult instanceof Number) {
								final int bValue = ((Number) ioResult).intValue();
								if (bValue == 0)
									ioResult = Boolean.FALSE;
								else if (bValue == 1)
									ioResult = Boolean.TRUE;
								else
									// IGNORE OTHER VALUES
									ioResult = null;
							}
						}
					} else if (operator == OSQLFilterFieldOperator.ASDATE.id) {
						if (ioResult != null) {
							if (ioResult instanceof Long)
								ioResult = new Date((Long) ioResult);
							else
								ioResult = iDatabase.getStorage().getConfiguration().getDateFormatInstance().parse(ioResult.toString());
						}
					} else if (operator == OSQLFilterFieldOperator.ASDATETIME.id) {
						if (ioResult != null) {
							if (ioResult instanceof Long)
								ioResult = new Date((Long) ioResult);
							else
								ioResult = iDatabase.getStorage().getConfiguration().getDateTimeFormatInstance().parse(ioResult.toString());
						}
					} else if (operator == OSQLFilterFieldOperator.TOJSON.id)
						ioResult = ioResult != null && ioResult instanceof ODocument ? ((ODocument) ioResult).toJSON() : null;

					else if (operator == OSQLFilterFieldOperator.KEYS.id)
						ioResult = ioResult != null && ioResult instanceof Map<?, ?> ? ((Map<?, ?>) ioResult).keySet() : null;

					else if (operator == OSQLFilterFieldOperator.VALUES.id)
						ioResult = ioResult != null && ioResult instanceof Map<?, ?> ? ((Map<?, ?>) ioResult).values() : null;
				}
			} catch (ParseException e) {
				OLogManager.instance().exception("Error on conversion of value '%s' using field operator %s", e,
						OCommandExecutionException.class, ioResult, OSQLFilterFieldOperator.getById(operator));
			}
		}

		return ioResult;
	}

	protected Object getRecordAttribute(final ODatabaseRecord iDatabase, final OIdentifiable iIdentifiable, String iFieldName) {
		iFieldName = iFieldName.toUpperCase();

		Object result = null;

		if (iFieldName.charAt(0) == '@') {
			if (iFieldName.equals("@THIS"))
				result = iIdentifiable;

			else if (iFieldName.equals("@RID"))
				result = iIdentifiable.getIdentity();

			else if (iFieldName.equals("@VERSION")) {
				result = iDatabase.getRecord(iIdentifiable).getVersion();

			} else if (iFieldName.equals("@CLASS")) {
				final ORecord<?> record = iDatabase.getRecord(iIdentifiable);
				if (record instanceof ODocument)
					result = ((ODocument) record).getClassName();

			} else if (iFieldName.equals("@TYPE"))
				result = ORecordFactory.getRecordTypeName(iDatabase.getRecord(iIdentifiable).getRecordType());

			else if (iFieldName.equals("@FIELDS")) {
				final ORecord<?> record = iDatabase.getRecord(iIdentifiable);
				if (record instanceof ODocument)
					result = ((ODocument) record).fieldNames();
			}

			else if (iFieldName.equals("@SIZE")) {
				final byte[] stream = iDatabase.getRecord(iIdentifiable).toStream();
				if (stream != null)
					result = stream.length;
			}
		}
		return result;
	}

	public boolean hasChainOperators() {
		return operationsChain != null;
	}

	@Override
	public String toString() {
		final StringBuilder buffer = new StringBuilder();
		if (name != null)
			buffer.append(name);
		if (operationsChain != null) {
			for (OPair<Integer, List<String>> op : operationsChain) {
				buffer.append('.');
				buffer.append(OSQLFilterFieldOperator.getById(op.getKey()));
				buffer.append(op.getValue());
			}
		}
		return buffer.toString();
	}

	public String getName() {
		return name;
	}
}
