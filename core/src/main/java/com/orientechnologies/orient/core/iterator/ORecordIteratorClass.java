/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.iterator;

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Iterator class to browse forward and backward the records of a cluster. Once browsed in a direction, the iterator cannot change
 * it. This iterator with "live updates" set is able to catch updates to the cluster sizes while browsing. This is the case when
 * concurrent clients/threads insert and remove item in any cluster the iterator is browsing. If the cluster are hot removed by from
 * the database the iterator could be invalid and throw exception of cluster not found.
 * 
 * @author Luca Garulli
 */
public class ORecordIteratorClass<REC extends ORecordInternal<?>> extends ORecordIteratorClusters<REC> {
  protected final OClass targetClass;
  protected boolean      polymorphic;
  protected boolean      useCache;

  /**
   * This method is only to maintain the retro compatibility with TinkerPop BP 2.2
   */
  public ORecordIteratorClass(final ODatabaseRecord iDatabase, final ODatabaseRecordAbstract iLowLevelDatabase,
      final String iClassName, final boolean iPolymorphic) {
    this(iDatabase, iLowLevelDatabase, iClassName, iPolymorphic, true, false);
  }

  public ORecordIteratorClass(final ODatabaseRecord iDatabase, final ODatabaseRecord iLowLevelDatabase, final String iClassName,
      final boolean iPolymorphic) {
    this(iDatabase, iLowLevelDatabase, iClassName, iPolymorphic, true, false);
  }

  public ORecordIteratorClass(final ODatabaseRecord iDatabase, final ODatabaseRecord iLowLevelDatabase, final String iClassName,
      final boolean iPolymorphic, final boolean iUseCache, final boolean iterateThroughTombstones) {
    super(iDatabase, iLowLevelDatabase, iUseCache, iterateThroughTombstones);

    targetClass = database.getMetadata().getSchema().getClass(iClassName);
    if (targetClass == null)
      throw new IllegalArgumentException("Class '" + iClassName + "' was not found in database schema");

    polymorphic = iPolymorphic;
    clusterIds = polymorphic ? targetClass.getPolymorphicClusterIds() : targetClass.getClusterIds();
    clusterIds = readableClusters(iDatabase, clusterIds);

    config();
  }
  
  
  private int[] readableClusters(ODatabaseRecord iDatabase,
		int[] clusterIds) {
	List<Integer> listOfReadableIds = new ArrayList<Integer>();
	
	for (int clusterId : clusterIds) {
		try {
			String clusterName = iDatabase.getClusterNameById(clusterId);
		    iDatabase.checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, clusterName);
		    listOfReadableIds.add(clusterId);
		}
		catch(OSecurityAccessException securityException) {
			// if the cluster is inaccessible it's simply not processed in the list.add
		}
	}
	
	int[] readableClusterIds = new int[listOfReadableIds.size()];
	int index = 0;
	for (int clusterId : listOfReadableIds) {
		readableClusterIds[index++] = clusterId;
	}
	
	return readableClusterIds;
}

@SuppressWarnings("unchecked")
  @Override
  public REC next() {
    final OIdentifiable rec = super.next();
    if (rec == null)
      return null;
    return (REC) rec.getRecord();
  }

  @SuppressWarnings("unchecked")
  @Override
  public REC previous() {
    final OIdentifiable rec = super.previous();
    if (rec == null)
      return null;

    return (REC) rec.getRecord();
  }

  @Override
  protected boolean include(final ORecord<?> record) {
    return record instanceof ODocument && targetClass.isSuperClassOf(((ODocument) record).getSchemaClass());
  }

  public boolean isPolymorphic() {
    return polymorphic;
  }

  @Override
  public String toString() {
    return String.format("ORecordIteratorClass.targetClass(%s).polymorphic(%s)", targetClass, polymorphic);
  }
}
