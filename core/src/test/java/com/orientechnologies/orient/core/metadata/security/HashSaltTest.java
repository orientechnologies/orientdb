package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.security.OSecurityManager;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

/**
 * Tests the salt + hash of passwords.
 * 
 * @author Luca Garulli
 */
public class HashSaltTest {

  @Test
  public void testSalt() throws InvalidKeySpecException, NoSuchAlgorithmException {
    final String password = "OrientDBisCool";
    final String hashed = OSecurityManager.createHashWithSalt(password);

    Assert.assertTrue(OSecurityManager.checkPasswordWithSalt(password, hashed));
  }

}
