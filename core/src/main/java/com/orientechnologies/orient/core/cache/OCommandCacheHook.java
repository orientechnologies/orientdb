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
package com.orientechnologies.orient.core.cache;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.hook.ORecordHookAbstract;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Hook that takes care to invalidate query cache as soon any change happen on database.
 * 
 * @author Luca Garulli
 */
public class OCommandCacheHook extends ORecordHookAbstract {

  private final OCommandCache     cmdCache;
  private final ODatabaseDocument database;

  public OCommandCacheHook(final ODatabaseDocument iDatabase) {
    database = iDatabase;
    cmdCache = iDatabase.getMetadata().getCommandCache().isEnabled() ? iDatabase.getMetadata().getCommandCache() : null;
  }

  @Override
  public void onRecordAfterCreate(final ORecord iRecord) {
    if (cmdCache == null)
      return;

    invalidateCache(iRecord);
  }

  @Override
  public void onRecordAfterUpdate(final ORecord iRecord) {
    if (cmdCache == null)
      return;

    invalidateCache(iRecord);
  }

  @Override
  public void onRecordAfterDelete(final ORecord iRecord) {
    if (cmdCache == null)
      return;

    invalidateCache(iRecord);
  }

  protected void invalidateCache(final ORecord iRecord) {
    if (cmdCache.getEvictStrategy() == OCommandCacheSoftRefs.STRATEGY.PER_CLUSTER)
      cmdCache.invalidateResultsOfCluster(database.getClusterNameById(iRecord.getIdentity().getClusterId()));
    else
      cmdCache.invalidateResultsOfCluster(null);
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.BOTH;
  }
}
