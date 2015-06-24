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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Hook that takes care to invalidate query cache as soon any change happen on database.
 * 
 * @author Luca Garulli
 */
public class OCommandCacheHook extends ODocumentHookAbstract {

  private final OCommandCache cmdCache;

  public OCommandCacheHook(final ODatabaseDocumentTx iDatabase) {
    super(iDatabase);
    cmdCache = database.getMetadata().getCommandCache().isEnabled() ? database.getMetadata().getCommandCache() : null;
  }

  @Override
  public void onRecordAfterCreate(final ODocument iDocument) {
    if (cmdCache == null)
      return;

    invalidateCache(iDocument);
  }

  @Override
  public void onRecordAfterUpdate(final ODocument iDocument) {
    if (cmdCache == null)
      return;

    invalidateCache(iDocument);
  }

  @Override
  public void onRecordAfterDelete(final ODocument iDocument) {
    if (cmdCache == null)
      return;

    invalidateCache(iDocument);
  }

  protected void invalidateCache(final ODocument iDocument) {
    cmdCache.invalidateResultsOfCluster(database.getClusterNameById(iDocument.getIdentity().getClusterId()));
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.BOTH;
  }
}
