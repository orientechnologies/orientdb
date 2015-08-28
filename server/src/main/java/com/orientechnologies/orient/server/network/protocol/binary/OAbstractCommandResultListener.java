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

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.fetch.OFetchListener;
import com.orientechnologies.orient.core.fetch.OFetchPlan;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchContext;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Abstract class to manage command results.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public abstract class OAbstractCommandResultListener implements OCommandResultListener {

  private OFetchPlan fetchPlan;

  public abstract boolean isEmpty();

  @Override
  public void end() {
  }

  public void setFetchPlan(final String iText) {
    fetchPlan = OFetchHelper.buildFetchPlan(iText);
  }

  protected void fetchRecord(final Object iRecord, final OFetchListener iFetchListener) {
    if (fetchPlan != null && fetchPlan != OFetchHelper.DEFAULT_FETCHPLAN && iRecord instanceof ORecord) {
      final ORecord record = (ORecord) iRecord;
      final OFetchContext context = new ORemoteFetchContext();
      OFetchHelper.fetch(record, record, fetchPlan, iFetchListener, context, "");
    }
  }
}
