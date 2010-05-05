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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.query.OQueryHelper;

/**
 * Represent an object field as value in the query condition.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OSQLFilterItemAbstract implements OSQLFilterItem {
	protected String													name;
	protected List<OPair<Integer, String[]>>	operationsChain	= null;

	public OSQLFilterItemAbstract(final OSQLFilter iQueryCompiled, final String iName) {
		int pos = iName.indexOf(OSQLFilterFieldOperator.CHAIN_SEPARATOR);
		if (pos > -1) {
			// GET ALL SPECIAL OPERATIONS
			name = iName.substring(0, pos);

			String part = iName;
			String partUpperCase = part.toUpperCase();

			while (pos > -1) {
				part = part.substring(pos + OSQLFilterFieldOperator.CHAIN_SEPARATOR.length());
				partUpperCase = partUpperCase.substring(pos + OSQLFilterFieldOperator.CHAIN_SEPARATOR.length());

				for (OSQLFilterFieldOperator op : OSQLFilterFieldOperator.OPERATORS)
					if (partUpperCase.startsWith(op.keyword)) {
						final String arguments[];

						if (op.minArguments > 0) {
							arguments = OQueryHelper.getParameters(part);
							if (arguments.length < op.minArguments || arguments.length > op.maxArguments)
								throw new OQueryParsingException(iQueryCompiled.text, "Syntax error: field operator '" + op.keyword + "' needs "
										+ (op.minArguments == op.maxArguments ? op.minArguments : op.minArguments + "-" + op.maxArguments)
										+ " argument(s) while has been received " + arguments.length, iQueryCompiled.currentPos + pos);
						} else
							arguments = null;

						// SPECIAL OPERATION FOUND: ADD IT IN TO THE CHAIN
						if (operationsChain == null)
							operationsChain = new ArrayList<OPair<Integer, String[]>>();

						operationsChain.add(new OPair<Integer, String[]>(op.id, arguments));

						pos = partUpperCase.indexOf(OQueryHelper.CLOSED_BRACE) + OSQLFilterFieldOperator.CHAIN_SEPARATOR.length();
						break;
					}

				if (operationsChain == null)
					throw new OQueryParsingException(iQueryCompiled.text,
							"Syntax error: field operator not recognized between the supported ones: "
									+ Arrays.toString(OSQLFilterFieldOperator.OPERATORS), iQueryCompiled.currentPos + pos);

				if (pos >= partUpperCase.length())
					return;

				pos = partUpperCase.indexOf(OSQLFilterFieldOperator.CHAIN_SEPARATOR, pos);
			}
		} else
			name = iName;
	}

	public Object transformValue(Object iResult) {
		if (iResult != null && operationsChain != null) {
			// APPLY OPERATIONS FOLLOWING THE STACK ORDER
			int operator;
			for (OPair<Integer, String[]> op : operationsChain) {
				operator = op.key.intValue();

				// NO ARGS OPERATORS
				if (operator == OSQLFilterFieldOperator.SIZE.id)
					iResult = iResult != null ? ((Collection<?>) iResult).size() : 0;

				else if (operator == OSQLFilterFieldOperator.LENGTH.id)
					iResult = iResult != null ? iResult.toString().length() : 0;

				else if (operator == OSQLFilterFieldOperator.TOUPPERCASE.id)
					iResult = iResult != null ? iResult.toString().toUpperCase() : 0;

				else if (operator == OSQLFilterFieldOperator.TOLOWERCASE.id)
					iResult = iResult != null ? iResult.toString().toLowerCase() : 0;

				else if (operator == OSQLFilterFieldOperator.TRIM.id)
					iResult = iResult != null ? iResult.toString().trim() : null;

				// OTHER OPERATORS
				else if (operator == OSQLFilterFieldOperator.CHARAT.id) {
					int index = Integer.parseInt(op.value[0]);
					iResult = iResult != null ? iResult.toString().substring(index, index + 1) : null;

				} else if (operator == OSQLFilterFieldOperator.INDEXOF.id && op.value[0].length() > 2) {
					String toFind = op.value[0].substring(1, op.value[0].length() - 1);
					int startIndex = op.value.length > 1 ? Integer.parseInt(op.value[1]) : 0;
					iResult = iResult != null ? iResult.toString().indexOf(toFind, startIndex) : null;

				} else if (operator == OSQLFilterFieldOperator.SUBSTRING.id) {

					int endIndex = op.value.length > 1 ? Integer.parseInt(op.value[1]) : op.value[0].length();
					iResult = iResult != null ? iResult.toString().substring(Integer.parseInt(op.value[0]), endIndex) : null;
				} else if (operator == OSQLFilterFieldOperator.FORMAT.id)

					iResult = iResult != null ? String.format(op.value[0], iResult) : null;

				else if (operator == OSQLFilterFieldOperator.LEFT.id) {
					final int len = Integer.parseInt(op.value[0]);
					iResult = iResult != null ? iResult.toString().substring(0,
							len <= iResult.toString().length() ? len : iResult.toString().length()) : null;
				} else if (operator == OSQLFilterFieldOperator.RIGHT.id) {
					final int offset = Integer.parseInt(op.value[0]);
					iResult = iResult != null ? iResult.toString().substring(
							offset <= iResult.toString().length() - 1 ? offset : iResult.toString().length() - 1) : null;
				}
			}
		}

		return iResult;
	}

	public boolean hasChainOperators() {
		return operationsChain != null;
	}

	@Override
	public String toString() {
		return name != null ? name : "";
	}

	public String getName() {
		return name;
	}
}
