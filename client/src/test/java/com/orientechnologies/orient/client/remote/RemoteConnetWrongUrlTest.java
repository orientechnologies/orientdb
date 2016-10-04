package com.orientechnologies.orient.client.remote;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;

public class RemoteConnetWrongUrlTest {

  @Test(expectedExceptions = ODatabaseException.class)
  public void testConnectWrongUrl() {
    ODatabaseDocument doc = new ODatabaseDocumentTx("remote:wrong:2424/test");
    doc.open("user", "user");

  }

}
