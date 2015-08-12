package com.orientechnologies.orient.core.compression.impl;

import javax.crypto.Cipher;

import com.orientechnologies.orient.core.exception.OSecurityException;

/***
 * (https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html). Issue
 * https://github.com/orientechnologies/orientdb/issues/89.
 * 
 * @author giastfader
 *
 */
public abstract class OAbstractEncryptedCompression extends OAbstractCompression {
  /***
   *
   * @param mode
   *          it can be Cipher.ENCRYPT_MODE or Cipher.DECRYPT_MODE
   * @param input
   * @param offset
   * @param length
   * @return
   * @throws Throwable
   */
  public abstract byte[] encryptOrDecrypt(int mode, byte[] input, int offset, int length) throws Throwable;

  @Override
  public byte[] compress(final byte[] content, final int offset, final int length) {
    try {
      return encryptOrDecrypt(Cipher.ENCRYPT_MODE, content, offset, length);
    } catch (Throwable e) {
      throw new OSecurityException("Cannot encrypt content", e);
    }
  };

  @Override
  public byte[] uncompress(final byte[] content, final int offset, final int length) {
    try {
      return encryptOrDecrypt(Cipher.DECRYPT_MODE, content, offset, length);
    } catch (Throwable e) {
      throw new OSecurityException("Cannot decrypt content", e);
    }
  };
}
