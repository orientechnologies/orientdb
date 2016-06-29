package com.orientechnologies.orient.core.compression.impl;

import org.junit.Test;
/**
 * @author Andrey Lomakin
 * @since 05.06.13
 */
public class NothingCompressionTest extends AbstractCompressionTest {
  public void testNothingCompression() {
    testCompression(ONothingCompression.NAME);
  }
}
