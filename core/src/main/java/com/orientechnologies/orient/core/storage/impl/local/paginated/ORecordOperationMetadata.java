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
package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;
import java.util.HashSet;
import java.util.Set;

/**
 * This class is used inside of {@link OPaginatedCluster} class as container for the records ids
 * which were changed during active atomic operation.
 *
 * @see OGlobalConfiguration#STORAGE_TRACK_CHANGED_RECORDS_IN_WAL
 */
public class ORecordOperationMetadata implements OAtomicOperationMetadata<Set<ORID>> {
  public static final String RID_METADATA_KEY = "cluster.record.rid";

  private final Set<ORID> rids = new HashSet<>();

  public void addRid(ORID rid) {
    rids.add(rid);
  }

  @Override
  public String getKey() {
    return RID_METADATA_KEY;
  }

  @Override
  public Set<ORID> getValue() {
    return rids;
  }
}
