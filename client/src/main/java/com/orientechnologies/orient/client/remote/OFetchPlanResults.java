package com.orientechnologies.orient.client.remote;

import java.util.Set;

import com.orientechnologies.orient.core.record.ORecord;

public interface OFetchPlanResults {
  public Set<ORecord> getFetchedRecordsToSend();
}
