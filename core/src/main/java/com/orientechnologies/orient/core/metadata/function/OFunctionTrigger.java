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
package com.orientechnologies.orient.core.metadata.function;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;

/**
 * Update the in-memory function library.
 * 
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OFunctionTrigger extends ODocumentHookAbstract {

  public static final String CLASSNAME = "OFunction";

  public OFunctionTrigger(ODatabaseDocument database) {
    super(database);
  }

  @Override
  public SCOPE[] getScopes() {
    return new SCOPE[] { SCOPE.CREATE, SCOPE.UPDATE, SCOPE.DELETE };
  }

  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.TARGET_NODE;
  }

  @Override
  public RESULT onTrigger(TYPE iType, ORecord iRecord) {
    OImmutableClass clazz = null;
    if (iRecord instanceof ODocument)
      clazz = ODocumentInternal.getImmutableSchemaClass((ODocument) iRecord);
    if (clazz == null || !clazz.isFunction())
      return RESULT.RECORD_NOT_CHANGED;
    return super.onTrigger(iType, iRecord);
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
