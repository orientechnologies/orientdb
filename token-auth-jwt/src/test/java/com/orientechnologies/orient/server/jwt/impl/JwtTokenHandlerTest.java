package com.orientechnologies.orient.server.jwt.impl;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.metadata.security.jwt.OJwtHeader;
import com.orientechnologies.orient.core.metadata.security.jwt.OJwtPayload;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.jwt.impl.JwtTokenHandler;

public class JwtTokenHandlerTest {

  private static final OServerParameterConfiguration[] I_PARAMS = new OServerParameterConfiguration[] { new OServerParameterConfiguration(
                                                                    JwtTokenHandler.O_SIGN_KEY, "crappy key") };

  @Test
  public void testWebTokenCreationValidation() throws InvalidKeyException, NoSuchAlgorithmException, IOException {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:" + JwtTokenHandlerTest.class.getSimpleName());
    db.create();
    try {
      OSecurityUser original = db.getUser();
      JwtTokenHandler handler = new JwtTokenHandler();
      handler.config(null, I_PARAMS);
      byte[] token = handler.getSignedWebToken(db, original);

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

  @Test(expectedExceptions = Exception.class)
  public void testInvalidToken() throws InvalidKeyException, NoSuchAlgorithmException, IOException {
    JwtTokenHandler handler = new JwtTokenHandler();
    handler.config(null, I_PARAMS);
    handler.parseWebToken("random".getBytes());
  }

  @Test
  public void testSerializeDeserializeWebHeader() throws Exception {
    OJwtHeader header = new OrientJwtHeader();
    header.setType("Orient");
    header.setAlgorithm("some");
    header.setKeyId("the_key");
    JwtTokenHandler handler = new JwtTokenHandler();
    byte[] headerbytes = handler.serializeWebHeader(header);

    OJwtHeader des = handler.deserializeWebHeader(headerbytes);
    assertNotNull(des);
    assertEquals(header.getType(), des.getType());
    assertEquals(header.getKeyId(), des.getKeyId());
    assertEquals(header.getAlgorithm(), des.getAlgorithm());
    assertEquals(header.getType(), des.getType());

  }

  @Test
  public void testSerializeDeserializeWebPayload() throws Exception {
    OJwtPayload payload = new OrientJwtPayload();
    String ptype = "OrientDB";
    payload.setAudience("audiance");
    payload.setExpiry(1L);
    payload.setIssuedAt(2L);
    payload.setIssuer("orient");
    payload.setNotBefore(3L);
    payload.setSubject("the subject");
    payload.setTokenId("aaa");

    // payload.setKeyId("the_key");
    JwtTokenHandler handler = new JwtTokenHandler();
    byte[] payloadbytes = handler.serializeWebPayload(payload);

    OJwtPayload des = handler.deserializeWebPayload(ptype, payloadbytes);
    assertNotNull(des);
    assertEquals(payload.getAudience(), des.getAudience());
    assertEquals(payload.getExpiry(), des.getExpiry());
    assertEquals(payload.getIssuedAt(), des.getIssuedAt());
    assertEquals(payload.getIssuer(), des.getIssuer());
    assertEquals(payload.getNotBefore(), des.getNotBefore());
    assertEquals(payload.getTokenId(), des.getTokenId());

  }

  @Test
  public void testTokenForge() throws InvalidKeyException, NoSuchAlgorithmException, IOException {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:" + JwtTokenHandlerTest.class.getSimpleName());
    db.create();
    try {
      OSecurityUser original = db.getUser();
      JwtTokenHandler handler = new JwtTokenHandler();

      handler.config(null, I_PARAMS);
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
  public void testBinartTokenCreationValidation() throws InvalidKeyException, NoSuchAlgorithmException, IOException {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:" + JwtTokenHandlerTest.class.getSimpleName());
    db.create();
    try {
      OSecurityUser original = db.getUser();
      JwtTokenHandler handler = new JwtTokenHandler();
      handler.config(null, I_PARAMS);
      byte[] token = handler.getSignedBinaryToken(db, original);

      OToken tok = handler.parseBinaryToken(token);

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

}
