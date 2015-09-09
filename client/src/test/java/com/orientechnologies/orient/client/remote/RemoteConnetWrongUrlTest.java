package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OStorageException;
import org.testng.annotations.Test;

public class RemoteConnetWrongUrlTest {

  @Test(expectedExceptions = OStorageException.class)
  public void testConnectWrongUrl() {
    ODatabaseDocument doc = new ODatabaseDocumentTx("remote:wrong:2424/test");
    doc.open("user", "user");

  }

}
