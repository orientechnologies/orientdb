package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.record.ORecord;
import java.util.Set;

public interface OFetchPlanResults {
  public Set<ORecord> getFetchedRecordsToSend();
}
