package com.orientechnologies.orient.client.remote;

import static org.junit.Assert.assertNull;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import org.junit.Test;

public class RemoteConnetWrongUrlTest {

  @Test(expected = ODatabaseException.class)
  public void testConnectWrongUrl() {
    ODatabaseDocument doc = new ODatabaseDocumentTx("remote:wrong:2424/test");
    doc.open("user", "user");
  }

  @Test
  public void testConnectWrongUrlTL() {
    try {
      ODatabaseDocument doc = new ODatabaseDocumentTx("remote:wrong:2424/test");
      doc.open("user", "user");
    } catch (ODatabaseException e) {

    }
    assertNull(ODatabaseRecordThreadLocal.instance().getIfDefined());
  }
}
