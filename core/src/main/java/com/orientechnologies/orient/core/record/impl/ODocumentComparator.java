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
package com.orientechnologies.orient.core.record.impl;

import java.util.Comparator;
import java.util.List;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSelect;

/**
 * Comparator implementation class used by ODocumentSorter class to sort documents following dynamic criteria.
 * 
 * @author Luca Garulli
 * 
 */
public class ODocumentComparator implements Comparator<OIdentifiable> {
	private List<OPair<String, String>>	orderCriteria;

	public ODocumentComparator(final List<OPair<String, String>> iOrderCriteria) {
		this.orderCriteria = iOrderCriteria;
	}

	@SuppressWarnings("unchecked")
	public int compare(final OIdentifiable iDoc1, final OIdentifiable iDoc2) {
		Object fieldValue1;
		Object fieldValue2;

		int partialResult = 0;

		for (OPair<String, String> field : orderCriteria) {
			fieldValue1 = ((ODocument) iDoc1.getRecord()).field(field.getKey());
			if (fieldValue1 == null)
				return factor(-1, field.getValue());

			fieldValue2 = ((ODocument) iDoc2.getRecord()).field(field.getKey());
			if (fieldValue2 == null)
				return factor(1, field.getValue());

			if (!(fieldValue1 instanceof Comparable<?>))
				throw new IllegalArgumentException("Can't sort documents because the field '" + field.getKey() + "' is not comparable");

			partialResult = ((Comparable<Object>) fieldValue1).compareTo(fieldValue2);

			partialResult = factor(partialResult, field.getValue());

			if (partialResult != 0)
				break;

			// CONTINUE WITH THE NEXT FIELD
		}

		return partialResult;
	}

	private int factor(final int partialResult, final String iOrdering) {
		if (iOrdering.equals(OCommandExecutorSQLSelect.KEYWORD_DESC))
			// INVERT THE ORDERING
			return partialResult * -1;

		return partialResult;
	}

}
