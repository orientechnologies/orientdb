package com.orientechnologies.orient.core.compression.impl;

import org.junit.Test;

/**
 * @author Andrey Lomakin
 * @since 05.06.13
 */
public class GZIPCompressionTest extends AbstractCompressionTest {
  @Test
  public void testGZIPCompression() {
    testCompression(OGZIPCompression.NAME);
  }
}
