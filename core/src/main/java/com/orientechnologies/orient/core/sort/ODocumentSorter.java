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
package com.orientechnologies.orient.core.sort;

import java.util.Collections;
import java.util.List;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Helper class that sorts a list of ODocument instances following criterias.
 * 
 * @author Luca Garulli
 * 
 */
public class ODocumentSorter {
	public static void sort(List<OIdentifiable> ioResultSet, List<OPair<String, String>> iOrderCriteria) {
		Collections.sort(ioResultSet, new ODocumentComparator(iOrderCriteria));
	}
}
