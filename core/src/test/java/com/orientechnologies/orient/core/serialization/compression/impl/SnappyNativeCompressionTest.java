package com.orientechnologies.orient.core.serialization.compression.impl;

import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 06.06.13
 */
@Test
public class SnappyNativeCompressionTest extends AbstractCompressionTest {
  public void testSnappyNativeCompression() {
    testCompression(OSnappyNativeCompression.NAME);
  }
}
