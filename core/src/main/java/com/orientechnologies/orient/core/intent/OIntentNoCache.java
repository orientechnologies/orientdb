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

package com.orientechnologies.orient.core.intent;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

/** Disable cache. This is helpful with operation like UPDATE/DELETE of many records. */
public class OIntentNoCache implements OIntent {
  private boolean previousLocalCacheEnabled;
  private boolean previousRetainRecords;
  private boolean previousRetainObjects;

  public void begin(final ODatabaseDocumentInternal iDatabase) {
    ODatabaseInternal<?> ownerDb = iDatabase.getDatabaseOwner();

    if (ownerDb instanceof ODatabaseDocument) {
      previousRetainRecords = ((ODatabaseDocument) ownerDb).isRetainRecords();
      ((ODatabaseDocument) ownerDb).setRetainRecords(false);
    }

    while (ownerDb.getDatabaseOwner() != ownerDb) ownerDb = ownerDb.getDatabaseOwner();
  }

  public void end(final ODatabaseDocumentInternal iDatabase) {
    ODatabaseInternal<?> ownerDb = iDatabase.getDatabaseOwner();

    if (ownerDb instanceof ODatabaseDocument) {
      ((ODatabaseDocument) ownerDb).setRetainRecords(previousRetainRecords);
    }

    while (ownerDb.getDatabaseOwner() != ownerDb) ownerDb = ownerDb.getDatabaseOwner();
  }

  @Override
  public OIntent copy() {
    final OIntentNoCache copy = new OIntentNoCache();
    copy.previousLocalCacheEnabled = previousLocalCacheEnabled;
    copy.previousRetainRecords = previousRetainRecords;
    copy.previousRetainObjects = previousRetainObjects;
    return copy;
  }
}
