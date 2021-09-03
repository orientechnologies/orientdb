package com.orientechnologies.orient.core.security;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

/** Created by frank on 19/11/2015. */
public class OSecurityManagerTest {

  @Test
  public void shouldCheckPlainPasswordAgainstHash() throws Exception {

    String hash = OSecurityManager.createHash("password", OSecurityManager.HASH_ALGORITHM, true);

    assertThat(OSecurityManager.checkPassword("password", hash)).isTrue();

    hash = OSecurityManager.createHash("password", OSecurityManager.PBKDF2_ALGORITHM, true);

    assertThat(OSecurityManager.checkPassword("password", hash)).isTrue();
  }

  @Test
  public void shouldCheckHashedPasswordAgainstHash() throws Exception {

    String hash = OSecurityManager.createHash("password", OSecurityManager.HASH_ALGORITHM, true);
    assertThat(OSecurityManager.checkPassword(hash, hash)).isFalse();

    hash = OSecurityManager.createHash("password", OSecurityManager.PBKDF2_ALGORITHM, true);

    assertThat(OSecurityManager.checkPassword(hash, hash)).isFalse();
  }

  @Test
  public void shouldCheckPlainPasswordAgainstHashWithSalt() throws Exception {

    String hash = OSecurityManager.createHashWithSalt("password");

    assertThat(OSecurityManager.checkPasswordWithSalt("password", hash)).isTrue();
  }

  @Test
  public void shouldCheckHashedWithSalPasswordAgainstHashWithSalt() throws Exception {

    String hash = OSecurityManager.createHashWithSalt("password");
    assertThat(OSecurityManager.checkPasswordWithSalt(hash, hash)).isFalse();
  }
}
