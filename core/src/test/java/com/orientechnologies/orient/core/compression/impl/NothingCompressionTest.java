package com.orientechnologies.orient.core.compression.impl;

import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 05.06.13
 */
@Test
public class NothingCompressionTest extends AbstractCompressionTest {
  public void testNothingCompression() {
    testCompression(ONothingCompression.NAME);
  }
}
