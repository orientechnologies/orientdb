package com.orientechnologies.common.directmemory;

/**
 * Created with IntelliJ IDEA. User: artem Date: 4/16/13 Time: 9:43 PM To change this template use File | Settings | File Templates.
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
    pointer += arrayOffset;
    unsafe.copyMemory(null, pointer, array, arrayOffset + unsafe.arrayBaseOffset(byte[].class), length);
  }

  @Override
  public void set(long pointer, byte[] content, int length) {
    unsafe.copyMemory(content, unsafe.arrayBaseOffset(byte[].class), null, pointer, length);
  }
}
