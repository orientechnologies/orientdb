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
package com.orientechnologies.orient.core.metadata.function;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Update the in-memory function library.
 * 
 * @author Luca Garulli
 */
public class OFunctionTrigger extends ODocumentHookAbstract {

  public OFunctionTrigger(ODatabaseDocument database) {
    super(database);
    setIncludeClasses("OFunction");
  }

  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.TARGET_NODE;
  }

  @Override
  public void onRecordAfterCreate(final ODocument iDocument) {
    reloadLibrary();
  }

  @Override
  public void onRecordAfterUpdate(final ODocument iDocument) {
    reloadLibrary();
  }

  @Override
  public void onRecordAfterDelete(final ODocument iDocument) {
    reloadLibrary();
  }

  protected void reloadLibrary() {
    database.getMetadata().getFunctionLibrary().load();

    Orient.instance().getScriptManager().close(database.getName());
  }
}
