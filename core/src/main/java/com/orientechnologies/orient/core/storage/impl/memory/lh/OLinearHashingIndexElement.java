package com.orientechnologies.orient.core.storage.impl.memory.lh;

/**
 * @author Artem Loginov (logart) logart2007@gmail.com Date: 8/28/12 Time: 10:18 AM
 */
class OLinearHashingIndexElement {
  byte signature    = Byte.MAX_VALUE;
  byte displacement = (byte) 255;
}
