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

package com.orientechnologies.orient.server.network.protocol.binary;

import com.orientechnologies.orient.client.remote.OFetchPlanResults;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchContext;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchListener;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.HashSet;
import java.util.Set;

/**
 * Synchronous command result manager.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSyncCommandResultListener extends OAbstractCommandResultListener
    implements OFetchPlanResults {
  private final Set<ORecord> fetchedRecordsToSend = new HashSet<ORecord>();
  private final Set<ORecord> alreadySent = new HashSet<ORecord>();

  public OSyncCommandResultListener(final OCommandResultListener wrappedResultListener) {
    super(wrappedResultListener);
  }

  @Override
  public boolean result(final Object iRecord) {
    if (iRecord instanceof ORecord) {
      alreadySent.add((ORecord) iRecord);
      fetchedRecordsToSend.remove(iRecord);
    }

    if (wrappedResultListener != null)
      // NOTIFY THE WRAPPED LISTENER
      wrappedResultListener.result(iRecord);

    fetchRecord(
        iRecord,
        new ORemoteFetchListener() {
          @Override
          protected void sendRecord(ORecord iLinked) {
            if (!alreadySent.contains(iLinked)) fetchedRecordsToSend.add(iLinked);
          }
        });
    return true;
  }

  public Set<ORecord> getFetchedRecordsToSend() {
    return fetchedRecordsToSend;
  }

  public boolean isEmpty() {
    return false;
  }

  @Override
  public void linkdedBySimpleValue(ODocument doc) {

    ORemoteFetchListener listener =
        new ORemoteFetchListener() {
          @Override
          protected void sendRecord(ORecord iLinked) {
            if (!alreadySent.contains(iLinked)) fetchedRecordsToSend.add(iLinked);
          }

          @Override
          public void parseLinked(
              ODocument iRootRecord,
              OIdentifiable iLinked,
              Object iUserObject,
              String iFieldName,
              OFetchContext iContext)
              throws OFetchException {
            if (!(iLinked instanceof ORecordId)) {
              sendRecord(iLinked.getRecord());
            }
          }

          @Override
          public void parseLinkedCollectionValue(
              ODocument iRootRecord,
              OIdentifiable iLinked,
              Object iUserObject,
              String iFieldName,
              OFetchContext iContext)
              throws OFetchException {

            if (!(iLinked instanceof ORecordId)) {
              sendRecord(iLinked.getRecord());
            }
          }
        };
    final OFetchContext context = new ORemoteFetchContext();
    OFetchHelper.fetch(doc, doc, OFetchHelper.buildFetchPlan(""), listener, context, "");
  }
}
