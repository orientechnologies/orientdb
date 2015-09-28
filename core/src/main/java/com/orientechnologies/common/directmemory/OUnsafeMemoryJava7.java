/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.common.directmemory;

/**
 * @author logart (logart2007-at-gmail.com)
 * @since 15.04.2013
 */
public class OUnsafeMemoryJava7 extends OUnsafeMemory {

  @Override
  public byte[] get(long pointer, final int length) {
    final byte[] result = new byte[length];
    unsafe.copyMemory(null, pointer, result, unsafe.arrayBaseOffset(byte[].class), length);
    return result;
  }

  @Override
  public void get(long pointer, byte[] array, int arrayOffset, int length) {
    unsafe.copyMemory(null, pointer, array, arrayOffset + unsafe.arrayBaseOffset(byte[].class), length);
  }

  @Override
  public void set(long pointer, byte[] content, int arrayOffset, int length) {
    unsafe.copyMemory(content, unsafe.arrayBaseOffset(byte[].class) + arrayOffset, null, pointer, length);

  }
}
