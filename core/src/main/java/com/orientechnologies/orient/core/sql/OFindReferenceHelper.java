/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.object.OLazyObjectListInterface;
import com.orientechnologies.orient.core.db.object.OLazyObjectMapInterface;
import com.orientechnologies.orient.core.db.object.OLazyObjectSetInterface;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Helper class to find reference in records.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @author Luca Molino
 * 
 */
public class OFindReferenceHelper {

  public static List<ODocument> findReferences(final Set<ORID> iRecordIds, final String classList) {
    final ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();

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

  private static void browseCluster(final ODatabaseDocument iDatabase, final Set<ORID> iSourceRIDs, final Map<ORID, Set<ORID>> map,
      final String iClusterName) {
    for (ORecord record : iDatabase.browseCluster(iClusterName)) {
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

  private static void browseClass(final ODatabaseDocument db, Set<ORID> iSourceRIDs, final Map<ORID, Set<ORID>> map,
      final String iClassName) {
    final OClass clazz = ((OMetadataInternal)db.getMetadata()).getImmutableSchemaSnapshot().getClass(iClassName);

    if (clazz == null)
      throw new OCommandExecutionException("Class '" + iClassName + "' was not found");

    for (int i : clazz.getClusterIds()) {
      browseCluster(db, iSourceRIDs, map, db.getClusterNameById(i));
    }
  }

  private static void checkObject(final Set<ORID> iSourceRIDs, final Map<ORID, Set<ORID>> map, final Object value,
      final ORecord iRootObject) {
    if (value instanceof OIdentifiable) {
      checkRecord(iSourceRIDs, map, (OIdentifiable) value, iRootObject);
    } else if (value instanceof Collection<?>) {
      checkCollection(iSourceRIDs, map, (Collection<?>) value, iRootObject);
    } else if (value instanceof Map<?, ?>) {
      checkMap(iSourceRIDs, map, (Map<?, ?>) value, iRootObject);
    }
  }

  private static void checkCollection(final Set<ORID> iSourceRIDs, final Map<ORID, Set<ORID>> map, final Collection<?> values,
      final ORecord iRootObject) {
    final Iterator<?> it;
    if (values instanceof OLazyObjectListInterface<?>) {
      ((OLazyObjectListInterface<?>) values).setConvertToRecord(false);
      it = ((OLazyObjectListInterface<?>) values).listIterator();
    } else if (values instanceof OLazyObjectSetInterface) {
      ((OLazyObjectSetInterface<?>) values).setConvertToRecord(false);
      it = ((OLazyObjectSetInterface<?>) values).iterator();
    } else if (values instanceof ORecordLazyMultiValue) {
      it = ((ORecordLazyMultiValue) values).rawIterator();
    } else {
      it = values.iterator();
    }
    while (it.hasNext()) {
      checkObject(iSourceRIDs, map, it.next(), iRootObject);
    }
  }

  private static void checkMap(final Set<ORID> iSourceRIDs, final Map<ORID, Set<ORID>> map, final Map<?, ?> values,
      final ORecord iRootObject) {
    final Iterator<?> it;
    if (values instanceof OLazyObjectMapInterface<?>) {
      ((OLazyObjectMapInterface<?>) values).setConvertToRecord(false);
      it = ((OLazyObjectMapInterface<?>) values).values().iterator();
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
      final ORecord iRootObject) {
    if (iSourceRIDs.contains(value.getIdentity())) {
      map.get(value.getIdentity()).add(iRootObject.getIdentity());
    }else if(!value.getIdentity().isValid() && value.getRecord() instanceof ODocument){
      //embedded document
      ODocument doc = value.getRecord();
      for (String fieldName : doc.fieldNames()) {
        Object fieldValue = doc.field(fieldName);
        checkObject(iSourceRIDs, map, fieldValue, iRootObject);
      }
    }
  }
}
