package com.orientechnologies.orient.core.compression.impl;

import org.testng.annotations.Test;

/**
 * @author giastfader@github
 * @since 22.04.2015
 */
@Test
public class OEncryptedCompressionTest extends AbstractCompressionTest {
  public void testAESEncryptedCompression() {
    testCompression(OEncryptedCompression.NAME);
  }
}
