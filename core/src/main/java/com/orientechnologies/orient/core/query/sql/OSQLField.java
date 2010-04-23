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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.query.OQueryHelper;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.ORecord.STATUS;

/**
 * Represent an object field as value in the query condition.
 * 
 * @author luca
 * 
 */
public class OSQLField implements OSQLValue {
	private String													name;
	private List<OPair<Integer, String[]>>	operationsChain	= null;

	public OSQLField(final OSQLQueryCompiled iQueryCompiled, final String iName) {
		int pos = iName.indexOf(OSQLFieldOperator.CHAIN_SEPARATOR);
		if (pos > -1) {
			// GET ALL SPECIAL OPERATIONS
			name = iName.substring(0, pos);

			String part = iName;
			String partUpperCase = part.toUpperCase();

			while (pos > -1) {
				partUpperCase = partUpperCase.substring(pos + OSQLFieldOperator.CHAIN_SEPARATOR.length());

				for (OSQLFieldOperator op : OSQLFieldOperator.OPERATORS)
					if (partUpperCase.startsWith(op.keyword)) {
						final String arguments[];

						if (op.arguments > 0) {
							arguments = OQueryHelper.getParameters(part);
							if (arguments.length != op.arguments)
								throw new OQueryParsingException(iQueryCompiled.text, "Syntax error: field operator '" + op.keyword + "' needs "
										+ op.arguments + " arguments while has been received " + arguments.length, iQueryCompiled.currentPos + pos);
						} else
							arguments = null;

						// SPECIAL OPERATION FOUND: ADD IT IN TO THE CHAIN
						if (operationsChain == null)
							operationsChain = new ArrayList<OPair<Integer, String[]>>();

						operationsChain.add(new OPair<Integer, String[]>(op.id, arguments));

						pos = partUpperCase.indexOf(OQueryHelper.CLOSED_BRACE) + OSQLFieldOperator.CHAIN_SEPARATOR.length();
						break;
					}

				if (operationsChain == null)
					throw new OQueryParsingException(iQueryCompiled.text,
							"Syntax error: field operator not recognized between the supported ones: "
									+ Arrays.toString(OSQLFieldOperator.OPERATORS), iQueryCompiled.currentPos + pos);

				if (pos >= partUpperCase.length())
					return;

				pos = partUpperCase.indexOf(OSQLFieldOperator.CHAIN_SEPARATOR, pos);
			}
		} else
			name = iName;
	}

	public Object getValue(final ORecordInternal<?> iRecord) {
		if (iRecord.getStatus() == STATUS.NOT_LOADED)
			iRecord.load();

		Object result = ((ORecordSchemaAware<?>) iRecord).field(name);

		if (result != null && operationsChain != null) {
			// APPLY OPERATIONS FOLLOWING THE STACK ORDER
			int operator;
			for (OPair<Integer, String[]> op : operationsChain) {
				operator = op.key.intValue();

				// NO ARGS OPERATORS
				if (operator == OSQLFieldOperator.SIZE.id)
					result = result != null ? ((Collection<?>) result).size() : 0;

				else if (operator == OSQLFieldOperator.LENGTH.id)
					result = result != null ? result.toString().length() : 0;

				else if (operator == OSQLFieldOperator.TOUPPERCASE.id)
					result = result != null ? result.toString().toUpperCase() : 0;

				else if (operator == OSQLFieldOperator.TOLOWERCASE.id)
					result = result != null ? result.toString().toLowerCase() : 0;

				else if (operator == OSQLFieldOperator.TRIM.id)
					result = result != null ? result.toString().trim() : null;

				// OTHER OPERATORS
				else if (operator == OSQLFieldOperator.CHARAT.id) {
					int index = Integer.parseInt(op.value[0]);
					result = result != null ? result.toString().substring(index, index + 1) : null;
				} else if (operator == OSQLFieldOperator.INDEXOF.id && op.value[0].length() > 2) {
					String toFind = op.value[0].substring(1, op.value[0].length() - 1);
					result = result != null ? result.toString().indexOf(toFind) : null;
				} else if (operator == OSQLFieldOperator.SUBSTRING.id)
					result = result != null ? result.toString().substring(Integer.parseInt(op.value[0]), Integer.parseInt(op.value[1]))
							: null;
				else if (operator == OSQLFieldOperator.FORMAT.id)
					result = result != null ? String.format(op.value[0], result) : null;
				else if (operator == OSQLFieldOperator.LEFT.id)
					result = result != null ? result.toString().substring(0, Integer.parseInt(op.value[0])) : null;
				else if (operator == OSQLFieldOperator.RIGHT.id)
					result = result != null ? result.toString().substring(Integer.parseInt(op.value[0])) : null;
			}
		}

		return result;
	}

	@Override
	public String toString() {
		StringBuilder buffer = new StringBuilder();
		if (name != null)
			buffer.append(name);

		return buffer.toString();
	}
}
