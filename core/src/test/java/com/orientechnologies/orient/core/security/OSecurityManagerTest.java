package com.orientechnologies.orient.core.security;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 19/11/2015.
 */
public class OSecurityManagerTest {

  @Test
  public void shouldCheckPlainPasswordAgainstHash() throws Exception {

    OSecurityManager securityManager = OSecurityManager.instance();

    String hash = securityManager.createHash("password", OSecurityManager.HASH_ALGORITHM, true);

    assertThat(securityManager.checkPassword("password", hash)).isTrue();

    hash = securityManager.createHash("password", OSecurityManager.PBKDF2_ALGORITHM, true);

    assertThat(securityManager.checkPassword("password", hash)).isTrue();

  }

  @Test
  public void shouldCheckPlainPasswordAgainstHashWithSalt() throws Exception {

    OSecurityManager securityManager = OSecurityManager.instance();

    String hash = securityManager.createHashWithSalt("password");

    assertThat(securityManager.checkPasswordWithSalt("password", hash)).isTrue();

  }


}