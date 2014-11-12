package com.orientechnologies.orient.server.jwt;

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
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.jwt.impl.JwtTokenHandler;

public class JwtTokenHandlerTest {

  @Test
  public void testTokenCreation() throws InvalidKeyException, NoSuchAlgorithmException, IOException {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:" + JwtTokenHandlerTest.class.getSimpleName());
    db.create();
    OSecurityUser original = db.getUser();
    JwtTokenHandler handler = new JwtTokenHandler();
    handler.config(null, new OServerParameterConfiguration[] { new OServerParameterConfiguration(JwtTokenHandler.O_SIGN_KEY,
        "crappy key") });
    byte[] token = handler.getSignedWebToken(db, original);

    OToken tok = handler.parseWebToken(token);

    assertNotNull(tok);

    assertTrue(tok.getIsVerified());

    OUser user = tok.getUser(db);
    assertEquals(user.getName(), original.getName());
    boolean boole = handler.validateToken(tok, "open", db.getName());
    assertTrue(boole);
    assertTrue(tok.getIsValid());
  }
}
