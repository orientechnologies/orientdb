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
package com.orientechnologies.orient.core.sql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.object.OLazyObjectList;
import com.orientechnologies.orient.core.db.object.OLazyObjectMap;
import com.orientechnologies.orient.core.db.object.OLazyObjectSet;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * Helper class to find reference in records.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @author Luca Molino
 * 
 */
public class OFindReferenceHelper {

	public static List<ODocument> findReferences(final Set<ORID> iRecordIds, final String classList) {
		final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();

		final Map<ORID, Set<ORID>> map = new HashMap<ORID, Set<ORID>>();
		for (ORID rid : iRecordIds) {
			map.put(rid, new HashSet<ORID>());
		}

		if (classList == null || classList.isEmpty()) {
			for (String clusterName : db.getClusterNames()) {
				browseCluster(db, iRecordIds, map, clusterName);
			}
		} else {
			final List<String> classes = OStringSerializerHelper.smartSplit(classList, ',');
			for (String clazz : classes) {
				if (clazz.startsWith("CLUSTER:")) {
					browseCluster(db, iRecordIds, map, clazz.substring(clazz.indexOf("CLUSTER:") + "CLUSTER:".length()));
				} else {
					browseClass(db, iRecordIds, map, clazz);
				}
			}
		}

		final List<ODocument> result = new ArrayList<ODocument>();
		for (Entry<ORID, Set<ORID>> entry : map.entrySet()) {
			final ODocument doc = new ODocument();
			result.add(doc);

			doc.field("rid", entry.getKey());
			doc.field("referredBy", entry.getValue());
		}

		return result;
	}

	private static void browseCluster(final ODatabaseRecord iDatabase, final Set<ORID> iSourceRIDs, final Map<ORID, Set<ORID>> map,
			final String iClusterName) {
		for (ORecordInternal<?> record : iDatabase.browseCluster(iClusterName)) {
			if (record instanceof ODocument) {
				try {
					for (String fieldName : ((ODocument) record).fieldNames()) {
						Object value = ((ODocument) record).field(fieldName);
						checkObject(iSourceRIDs, map, value, (ODocument) record);
					}
				} catch (Exception e) {
					OLogManager.instance().debug(null, "Error reading record " + record.getIdentity(), e);
				}
			}
		}
	}

	private static void browseClass(final ODatabaseRecord db, Set<ORID> iSourceRIDs, final Map<ORID, Set<ORID>> map,
			final String iClassName) {
		final OClass clazz = db.getMetadata().getSchema().getClass(iClassName);

		if (clazz == null)
			throw new OCommandExecutionException("Class '" + iClassName + "' was not found");

		for (int i : clazz.getClusterIds()) {
			browseCluster(db, iSourceRIDs, map, db.getClusterNameById(i));
		}
	}

	private static void checkObject(final Set<ORID> iSourceRIDs, final Map<ORID, Set<ORID>> map, final Object value,
			final ORecord<?> iRootObject) {
		if (value instanceof OIdentifiable) {
			checkRecord(iSourceRIDs, map, (OIdentifiable) value, iRootObject);
		} else if (value instanceof Collection<?>) {
			checkCollection(iSourceRIDs, map, (Collection<?>) value, iRootObject);
		} else if (value instanceof Map<?, ?>) {
			checkMap(iSourceRIDs, map, (Map<?, ?>) value, iRootObject);
		}
	}

	private static void checkCollection(final Set<ORID> iSourceRIDs, final Map<ORID, Set<ORID>> map, final Collection<?> values,
			final ORecord<?> iRootObject) {
		final Iterator<?> it;
		if (values instanceof OLazyObjectList) {
			((OLazyObjectList<?>) values).setConvertToRecord(false);
			it = ((OLazyObjectList<?>) values).listIterator();
		} else if (values instanceof OLazyObjectSet) {
			((OLazyObjectSet<?>) values).setConvertToRecord(false);
			it = ((OLazyObjectSet<?>) values).iterator();
		} else if (values instanceof ORecordLazyList) {
			it = ((ORecordLazyList) values).rawIterator();
		} else if (values instanceof OMVRBTreeRIDSet) {
			it = ((OMVRBTreeRIDSet) values).iterator();
		} else {
			it = values.iterator();
		}
		while (it.hasNext()) {
			checkObject(iSourceRIDs, map, it.next(), iRootObject);
		}
	}

	private static void checkMap(final Set<ORID> iSourceRIDs, final Map<ORID, Set<ORID>> map, final Map<?, ?> values,
			final ORecord<?> iRootObject) {
		final Iterator<?> it;
		if (values instanceof OLazyObjectMap) {
			((OLazyObjectMap<?>) values).setConvertToRecord(false);
			it = ((OLazyObjectMap<?>) values).values().iterator();
		} else if (values instanceof ORecordLazyMap) {
			it = ((ORecordLazyMap) values).rawIterator();
		} else {
			it = values.values().iterator();
		}
		while (it.hasNext()) {
			checkObject(iSourceRIDs, map, it.next(), iRootObject);
		}
	}

	private static void checkRecord(final Set<ORID> iSourceRIDs, final Map<ORID, Set<ORID>> map, final OIdentifiable value,
			final ORecord<?> iRootObject) {
		if (iSourceRIDs.contains(value.getIdentity()))
			map.get(value.getIdentity()).add(iRootObject.getIdentity());
	}
}
