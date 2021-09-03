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
package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.ODatabasePoolBase;

/**
 * @deprecated use {@link com.orientechnologies.orient.core.db.OPartitionedDatabasePool} or {@link
 *     com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory} instead.
 */
@Deprecated
public class ODatabaseDocumentPool extends ODatabasePoolBase<ODatabaseDocumentTx> {

  private static ODatabaseDocumentPool globalInstance = new ODatabaseDocumentPool();

  public ODatabaseDocumentPool() {
    super();
  }

  public ODatabaseDocumentPool(
      final String iURL, final String iUserName, final String iUserPassword) {
    super(iURL, iUserName, iUserPassword);
  }

  public static ODatabaseDocumentPool global() {
    globalInstance.setup();
    return globalInstance;
  }

  public static ODatabaseDocumentPool global(final int iPoolMin, final int iPoolMax) {
    globalInstance.setup(iPoolMin, iPoolMax);
    return globalInstance;
  }

  @Override
  protected ODatabaseDocumentTx createResource(
      Object owner, String iDatabaseName, Object... iAdditionalArgs) {
    return new ODatabaseDocumentTxPooled(
        (ODatabaseDocumentPool) owner,
        iDatabaseName,
        (String) iAdditionalArgs[0],
        (String) iAdditionalArgs[1]);
  }
}
