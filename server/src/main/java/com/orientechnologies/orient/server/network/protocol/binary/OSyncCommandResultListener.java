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

package com.orientechnologies.orient.server.network.protocol.binary;

import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchListener;
import com.orientechnologies.orient.core.record.ORecord;

import java.util.HashSet;
import java.util.Set;

/**
 * Synchronous command result manager.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public class OSyncCommandResultListener extends OAbstractCommandResultListener {
  private final Set<ORecord> fetchedRecordsToSend = new HashSet<ORecord>();
  private final Set<ORecord> alreadySent          = new HashSet<ORecord>();

  @Override
  public boolean result(final Object iRecord) {
    if (iRecord instanceof ORecord) {
      alreadySent.add((ORecord) iRecord);
      fetchedRecordsToSend.remove(iRecord);
    }
    fetchRecord(iRecord, new ORemoteFetchListener() {
      @Override
      protected void sendRecord(ORecord iLinked) {
        if (!alreadySent.contains(iLinked))
          fetchedRecordsToSend.add(iLinked);
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
}
