package com.orientechnologies.orient.core.compression.impl;

import org.junit.Test;
/**
 * @author Andrey Lomakin
 * @since 05.06.13
 */
public class SnappyCompressionTest extends AbstractCompressionTest {
  @Test
  public void testSnappyCompression() {
    testCompression(OSnappyCompression.NAME);
  }
}
