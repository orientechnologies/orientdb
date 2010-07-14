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
package com.orientechnologies.orient.core.id.fetch;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Helper class for fetching.
 * 
 * @author Luca Garulli
 * 
 */
public class OFetchHelper {
	private static final String	ANY_FIELD	= "*";

	public static Map<String, Integer> buildFetchPlan(final String iFetchPlan) {
		final Map<String, Integer> fetchPlan;

		if (iFetchPlan != null) {
			// CHECK IF THERE IS SOME FETCH-DEPTH
			final String[] planParts = iFetchPlan.split(" ");
			fetchPlan = new HashMap<String, Integer>();

			String[] parts;
			for (String planPart : planParts) {
				parts = planPart.split(":");
				fetchPlan.put(parts[0], Integer.parseInt(parts[1]));
			}
		} else
			fetchPlan = null;

		return fetchPlan;
	}

	@SuppressWarnings("unchecked")
	public static void fetch(final ODocument iRootRecord, final Object iUserObject, final Map<String, Integer> iFetchPlan,
			final String iCurrentField, final int iCurrentLevel, final int iMaxFetch, final OFetchListener iListener) {

		if (iMaxFetch > -1 && iListener.size() >= iMaxFetch)
			// MAX FETCH SIZE REACHED: STOP TO FETCH AT ALL
			return;

		Object fieldValue;
		Integer depthLevel;
		int currentLevel;
		final Integer anyFieldDepthLevel = iFetchPlan != null ? iFetchPlan.get(ANY_FIELD) : -1;

		// BROWSE ALL THE DOCUMENT'S FIELDS
		for (String fieldName : iRootRecord.fieldNames()) {
			fieldValue = iRootRecord.field(fieldName);

			if (fieldValue == null
					|| !(fieldValue instanceof ODocument)
					&& (!(fieldValue instanceof Collection<?>) || ((Collection<?>) fieldValue).size() == 0 || !(((Collection<?>) fieldValue)
							.iterator().next() instanceof ODocument)))
				// NULL NEITHER LINK, NOR COLLECTION OF LINKS
				continue;

			depthLevel = getDepthLevel(iRootRecord, iFetchPlan, fieldName);

			if (depthLevel == null)
				// NO SPECIFIED: ASSIGN DEFAULT LEVEL TAKEN FROM * WILDCARD IF ANY
				depthLevel = anyFieldDepthLevel;

			if (depthLevel == 0)
				// NO FETCH THIS FIELD PLEASE
				continue;

			// DETERMINE CURRENT DEPTH LEVEL
			if (fieldName.equals(iCurrentField)) {
				currentLevel = iCurrentLevel + 1;

				if (depthLevel >= currentLevel)
					// MAX DEPTH REACHED: STOP TO FETCH THIS FIELD
					continue;
			} else
				currentLevel = 0;

			Object userObject;
			if (fieldValue instanceof ODocument) {
				final ODocument linked = (ODocument) fieldValue;
				userObject = iListener.fetchLinked(iRootRecord, iUserObject, fieldName, linked);
				if (userObject != null)
					// GO RECURSIVELY
					fetch(linked, userObject, iFetchPlan, fieldName, currentLevel, iMaxFetch, iListener);

			} else if (fieldValue instanceof Collection<?>) {
				final Collection<ODocument> linked = (Collection<ODocument>) fieldValue;
				userObject = iListener.fetchLinked(iRootRecord, iUserObject, fieldName, linked);
				// for (ODocument d : linked) {
				// if (userObject != null)
				// // GO RECURSIVELY
				// fetch(d, userObject, iFetchPlan, fieldName, currentLevel, iMaxFetch, iListener);
				// }
			}

			if (iMaxFetch > -1 && iListener.size() >= iMaxFetch)
				// MAX FETCH SIZE REACHED: STOP TO FETCH AT ALL
				return;
		}
	}

	private static Integer getDepthLevel(final ODocument doc, final Map<String, Integer> iFetchPlan, final String iFieldName) {
		Integer depthLevel;

		if (iFetchPlan != null) {
			// GET THE FETCH PLAN FOR THE GENERIC FIELD IF SPECIFIED
			depthLevel = iFetchPlan.get(iFieldName);

			if (depthLevel == null) {
				OClass cls = doc.getSchemaClass();
				while (cls != null && depthLevel == null) {
					depthLevel = iFetchPlan.get(cls.getName() + "." + iFieldName);

					if (depthLevel == null)
						cls = cls.getSuperClass();
				}
			}
		} else
			// INFINITE
			depthLevel = -1;

		return depthLevel;
	}
}
