package com.orientechnologies.orient.core.serialization.compression.impl;

import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 05.06.13
 */
@Test
public class GZIPCompressionTest extends AbstractCompressionTest {
  public void testGZIPCompression() {
    testCompression(OGZIPCompression.NAME);
  }
}
