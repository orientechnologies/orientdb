/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.iterator;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.storage.OStorage;
import java.util.Arrays;

/**
 * Iterator class to browse forward and backward the records of a cluster. Once browsed in a
 * direction, the iterator cannot change it. This iterator with "live updates" set is able to catch
 * updates to the cluster sizes while browsing. This is the case when concurrent clients/threads
 * insert and remove item in any cluster the iterator is browsing. If the cluster are hot removed by
 * from the database the iterator could be invalid and throw exception of cluster not found.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ORecordIteratorClass<REC extends ORecord> extends ORecordIteratorClusters<REC> {
  protected final OClass targetClass;
  protected boolean polymorphic;

  public ORecordIteratorClass(
      final ODatabaseDocumentInternal iDatabase,
      final String iClassName,
      final boolean iPolymorphic) {
    this(iDatabase, iClassName, iPolymorphic, true);
  }

  public ORecordIteratorClass(
      final ODatabaseDocumentInternal iDatabase,
      final String iClassName,
      final boolean iPolymorphic,
      boolean begin) {
    this(iDatabase, iClassName, iPolymorphic, OStorage.LOCKING_STRATEGY.DEFAULT);
    if (begin) begin();
  }

  @Deprecated
  public ORecordIteratorClass(
      final ODatabaseDocumentInternal iDatabase,
      final String iClassName,
      final boolean iPolymorphic,
      final OStorage.LOCKING_STRATEGY iLockingStrategy) {
    super(iDatabase, iLockingStrategy);

    targetClass =
        ((OMetadataInternal) database.getMetadata())
            .getImmutableSchemaSnapshot()
            .getClass(iClassName);
    if (targetClass == null)
      throw new IllegalArgumentException(
          "Class '" + iClassName + "' was not found in database schema");

    polymorphic = iPolymorphic;
    clusterIds = polymorphic ? targetClass.getPolymorphicClusterIds() : targetClass.getClusterIds();
    clusterIds = OClassImpl.readableClusters(iDatabase, clusterIds, targetClass.getName());

    checkForSystemClusters(iDatabase, clusterIds);

    Arrays.sort(clusterIds);
    config();
  }

  protected ORecordIteratorClass(
      final ODatabaseDocumentInternal database, OClass targetClass, boolean polymorphic) {
    super(database, targetClass.getPolymorphicClusterIds());
    this.targetClass = targetClass;
    this.polymorphic = polymorphic;
  }

  @SuppressWarnings("unchecked")
  @Override
  public REC next() {
    final OIdentifiable rec = super.next();
    if (rec == null) return null;
    return (REC) rec.getRecord();
  }

  @SuppressWarnings("unchecked")
  @Override
  public REC previous() {
    final OIdentifiable rec = super.previous();
    if (rec == null) return null;

    return (REC) rec.getRecord();
  }

  public boolean isPolymorphic() {
    return polymorphic;
  }

  @Override
  public String toString() {
    return String.format(
        "ORecordIteratorClass.targetClass(%s).polymorphic(%s)", targetClass, polymorphic);
  }

  @Override
  protected boolean include(final ORecord record) {
    return record instanceof ODocument
        && targetClass.isSuperClassOf(
            ODocumentInternal.getImmutableSchemaClass(((ODocument) record)));
  }

  public OClass getTargetClass() {
    return targetClass;
  }

  @Override
  protected void config() {
    currentClusterIdx = 0; // START FROM THE FIRST CLUSTER

    updateClusterRange();

    totalAvailableRecords = database.countClusterElements(clusterIds);

    txEntries = database.getTransaction().getNewRecordEntriesByClass(targetClass, polymorphic);

    if (txEntries != null)
      // ADJUST TOTAL ELEMENT BASED ON CURRENT TRANSACTION'S ENTRIES
      for (ORecordOperation entry : txEntries) {
        if (!entry.getRecord().getIdentity().isPersistent()
            && entry.type != ORecordOperation.DELETED) totalAvailableRecords++;
        else if (entry.type == ORecordOperation.DELETED) totalAvailableRecords--;
      }
  }
}
