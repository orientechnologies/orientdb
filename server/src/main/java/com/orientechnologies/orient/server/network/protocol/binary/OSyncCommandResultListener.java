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
  private final Set<ORecord<?>> fetchedRecordsToSend = new HashSet<ORecord<?>>();

  @Override
  public boolean result(final Object iRecord) {
    fetchRecord(iRecord, new ORemoteFetchListener() {
      @Override
      protected void sendRecord(ORecord<?> iLinked) {
        fetchedRecordsToSend.add(iLinked);
      }
    });
    return true;
  }

  public Set<ORecord<?>> getFetchedRecordsToSend() {
    return fetchedRecordsToSend;
  }

  public boolean isEmpty() {
    return false;
  }
}
