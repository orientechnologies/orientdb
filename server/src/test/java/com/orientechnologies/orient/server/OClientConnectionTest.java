package com.orientechnologies.orient.server;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.token.OTokenHandlerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 27/12/15.
 */
public class OClientConnectionTest {

  private ODatabaseDocumentInternal db;

  @Before
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + OClientConnectionTest.class.getSimpleName());
    db.create();
  }


  @After
  public void after() {
    db.drop();
  }


  @Test
  public void testValidToken() throws IOException {
    OClientConnection conn = new OClientConnection(1, null);
    OTokenHandler handler = new OTokenHandlerImpl(null);
    byte[] tokenBytes = handler.getSignedBinaryToken(db, db.getUser(), conn.data);

    conn.validateSession(tokenBytes, handler);
    assertTrue(conn.tokenBased);
    assertEquals(tokenBytes, conn.tokenBytes);
    assertNotNull(conn.token);
  }

}
