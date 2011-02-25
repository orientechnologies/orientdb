/*
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
package com.orientechnologies.orient.core.db;

import java.util.ArrayDeque;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class ODatabaseRecordUtils {
//
//	public static void assignDatabase(final ORecord<?> iRecord, final ODatabaseRecord iDatabase) {
//		ORecordElement prev = null;
//		ORecordElement curr = iRecord;
//		ArrayDeque<Object[]> stack = new ArrayDeque<Object[]>();
//
//		while (true) {
//			curr.setDatabase(iDatabase);
//
//			if( !stack.isEmpty())
//				
//			if (curr instanceof ODocument && ((ODocument) curr).size() > 0) {
//				step = 'a';
//				array = ((ODocument) curr).fieldValues();
//				pos = 0;
//			}
//
//			
//			switch (step) {
//			case 'd': // DISCOVERY
//				if (curr instanceof ODocument && ((ODocument) curr).size() > 0) {
//					step = 'a';
//					array = ((ODocument) curr).fieldValues();
//					pos = 0;
//				}
//				break;
//
//			case 'a': // ARRAY
//				for (; pos < array.length; ++pos) {
//					if (array[pos] instanceof ORecordElement) {
//						curr = (ORecordElement) array[pos];
//						break;
//					}
//				}
//				
//				if( pos < array.length; ++pos) {
//
//				break;
//			}
//		}
//	}
}
