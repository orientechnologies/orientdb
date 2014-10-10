package com.orientechnologies.orient.core.sql.query;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.OClusterPositionLong;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OSqlQueryTest {

  @Test(expectedExceptions = ODatabaseException.class, expectedExceptionsMessageRegExp = ".*not.*persistent.*")
  public void testNotPersistentParameters() {
    ODocument doc = new ODocument(new ORecordId(1, new OClusterPositionLong(-2)));
    OSQLSynchQuery query = new OSQLSynchQuery<Object>("select from OUser");
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("aaa", doc);
    query.serializeQueryParameters(params);
  }

}
