/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.network.protocol.binary;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.fetch.OFetchListener;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchContext;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchListener;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Abstract class to manage command results.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OAbstractCommandResultListener implements OCommandResultListener {

  private final Set<ODocument> fetchedRecordsToSend = new HashSet<ODocument>();
  private Map<String, Integer> fetchPlan;

  public abstract boolean isEmpty();

  protected void fetchRecord(final Object iRecord) {
    if (fetchPlan != null && iRecord instanceof ORecordInternal<?>) {
      final ORecordInternal<?> record = (ORecordInternal<?>) iRecord;
      final OFetchListener listener = new ORemoteFetchListener(fetchedRecordsToSend);
      final OFetchContext context = new ORemoteFetchContext();
      OFetchHelper.fetch(record, record, fetchPlan, listener, context, "");
    }
  }

  @Override
  public void end() {
  }

  public void setFetchPlan(final String iText) {
    fetchPlan = iText != null ? OFetchHelper.buildFetchPlan(iText) : null;
  }

  public Set<ODocument> getFetchedRecordsToSend() {
    return fetchedRecordsToSend;
  }
}