package com.orientechnologies.orient.server.token;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.metadata.security.jwt.OJwtPayload;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenHeader;
import com.orientechnologies.orient.core.metadata.security.jwt.OrientJwtHeader;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import org.junit.Test;

public class OTokenHandlerImplTest {

  @Test
  public void testWebTokenCreationValidation()
      throws InvalidKeyException, NoSuchAlgorithmException, IOException {
    ODatabaseDocumentTx db =
        new ODatabaseDocumentTx("memory:" + OTokenHandlerImplTest.class.getSimpleName());
    db.create();
    try {
      OSecurityUser original = db.getUser();
      OTokenHandlerImpl handler = new OTokenHandlerImpl("any key".getBytes(), 60, "HmacSHA256");
      byte[] token = handler.getSignedWebToken(db, original);

      try {
        // Make this thread wait at least 10 milliseconds before check the validity
        Thread.sleep(10);
      } catch (InterruptedException e) {
      }

      OToken tok = handler.parseWebToken(token);

      assertNotNull(tok);

      assertTrue(tok.getIsVerified());

      OUser user = tok.getUser(db);
      assertEquals(user.getName(), original.getName());
      boolean boole = handler.validateToken(tok, "open", db.getName());
      assertTrue(boole);
      assertTrue(tok.getIsValid());
    } finally {
      db.drop();
    }
  }

  @Test(expected = Exception.class)
  public void testInvalidToken() throws InvalidKeyException, NoSuchAlgorithmException, IOException {
    OTokenHandlerImpl handler = new OTokenHandlerImpl("any key".getBytes(), 60, "HmacSHA256");
    handler.parseWebToken("random".getBytes());
  }

  @Test
  public void testSerializeDeserializeWebHeader() throws Exception {
    OTokenHeader header = new OrientJwtHeader();
    header.setType("Orient");
    header.setAlgorithm("some");
    header.setKeyId("the_key");
    OTokenHandlerImpl handler = new OTokenHandlerImpl();
    byte[] headerbytes = handler.serializeWebHeader(header);

    OTokenHeader des = handler.deserializeWebHeader(headerbytes);
    assertNotNull(des);
    assertEquals(header.getType(), des.getType());
    assertEquals(header.getKeyId(), des.getKeyId());
    assertEquals(header.getAlgorithm(), des.getAlgorithm());
    assertEquals(header.getType(), des.getType());
  }

  @Test
  public void testSerializeDeserializeWebPayload() throws Exception {
    OrientJwtPayload payload = new OrientJwtPayload();
    String ptype = "OrientDB";
    payload.setAudience("audiance");
    payload.setExpiry(1L);
    payload.setIssuedAt(2L);
    payload.setIssuer("orient");
    payload.setNotBefore(3L);
    payload.setUserName("the subject");
    payload.setTokenId("aaa");
    payload.setUserRid(new ORecordId(3, 4));

    OTokenHandlerImpl handler = new OTokenHandlerImpl();
    byte[] payloadbytes = handler.serializeWebPayload(payload);

    OJwtPayload des = handler.deserializeWebPayload(ptype, payloadbytes);
    assertNotNull(des);
    assertEquals(payload.getAudience(), des.getAudience());
    assertEquals(payload.getExpiry(), des.getExpiry());
    assertEquals(payload.getIssuedAt(), des.getIssuedAt());
    assertEquals(payload.getIssuer(), des.getIssuer());
    assertEquals(payload.getNotBefore(), des.getNotBefore());
    assertEquals(payload.getTokenId(), des.getTokenId());
    assertEquals(payload.getUserName(), des.getUserName());
  }

  @Test
  public void testTokenForge() throws InvalidKeyException, NoSuchAlgorithmException, IOException {
    ODatabaseDocumentTx db =
        new ODatabaseDocumentTx("memory:" + OTokenHandlerImplTest.class.getSimpleName());
    db.create();
    try {
      OSecurityUser original = db.getUser();
      OTokenHandlerImpl handler = new OTokenHandlerImpl("any key".getBytes(), 60, "HmacSHA256");

      byte[] token = handler.getSignedWebToken(db, original);
      byte[] token2 = handler.getSignedWebToken(db, original);
      String s = new String(token);
      String s2 = new String(token2);

      String newS = s.substring(0, s.lastIndexOf('.')) + s2.substring(s2.lastIndexOf('.'));

      OToken tok = handler.parseWebToken(newS.getBytes());

      assertNotNull(tok);

      assertFalse(tok.getIsVerified());
    } finally {
      db.drop();
    }
  }

  @Test
  public void testBinartTokenCreationValidation()
      throws InvalidKeyException, NoSuchAlgorithmException, IOException {
    ODatabaseDocumentTx db =
        new ODatabaseDocumentTx("memory:" + OTokenHandlerImplTest.class.getSimpleName());
    db.create();
    try {
      OSecurityUser original = db.getUser();
      OTokenHandlerImpl handler = new OTokenHandlerImpl("any key".getBytes(), 60, "HmacSHA256");
      ONetworkProtocolData data = new ONetworkProtocolData();
      data.driverName = "aa";
      data.driverVersion = "aa";
      data.setSerializationImpl("a");
      data.protocolVersion = 2;

      byte[] token = handler.getSignedBinaryToken(db, original, data);

      OToken tok = handler.parseBinaryToken(token);

      assertNotNull(tok);

      assertTrue(tok.getIsVerified());

      OUser user = tok.getUser(db);
      assertEquals(user.getName(), original.getName());
      boolean boole = handler.validateBinaryToken(tok);
      assertTrue(boole);
      assertTrue(tok.getIsValid());
    } finally {
      db.drop();
    }
  }

  @Test
  public void testTokenNotRenew() {
    ODatabaseDocumentTx db =
        new ODatabaseDocumentTx("memory:" + OTokenHandlerImplTest.class.getSimpleName());
    db.create();
    try {
      OSecurityUser original = db.getUser();
      OTokenHandlerImpl handler = new OTokenHandlerImpl("any key".getBytes(), 60, "HmacSHA256");
      ONetworkProtocolData data = new ONetworkProtocolData();
      data.driverName = "aa";
      data.driverVersion = "aa";
      data.setSerializationImpl("a");
      data.protocolVersion = 2;

      byte[] token = handler.getSignedBinaryToken(db, original, data);

      OToken tok = handler.parseBinaryToken(token);
      token = handler.renewIfNeeded(tok);

      assertEquals(0, token.length);

    } finally {
      db.drop();
    }
  }

  @Test
  public void testTokenRenew() {
    ODatabaseDocumentTx db =
        new ODatabaseDocumentTx("memory:" + OTokenHandlerImplTest.class.getSimpleName());
    db.create();
    try {
      OSecurityUser original = db.getUser();
      OTokenHandlerImpl handler = new OTokenHandlerImpl("any key".getBytes(), 60, "HmacSHA256");
      ONetworkProtocolData data = new ONetworkProtocolData();
      data.driverName = "aa";
      data.driverVersion = "aa";
      data.setSerializationImpl("a");
      data.protocolVersion = 2;

      byte[] token = handler.getSignedBinaryToken(db, original, data);

      OToken tok = handler.parseBinaryToken(token);
      tok.setExpiry(System.currentTimeMillis() + (handler.getSessionInMills() / 2) - 1);
      token = handler.renewIfNeeded(tok);

      assertTrue(token.length != 0);

    } finally {
      db.drop();
    }
  }
}
