/*
 * Copyright 2010-2013 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 *
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 *
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 *
 * For more information: http://www.orientechnologies.com
 */
package com.orientechnologies.agent;

import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("restriction")
public class OL {

  public static final  int           DELAY    = 60;
  public static final  String        AES      = "AES";
  private static       SecretKeySpec key;
  private static final byte[]        keyValue = new byte[] { 'T', 'h', 'e', 'B', 'e', 's', 't', 'S', 'e', 'c', 'r', 'e', 't', 'K',
      'e', 'y' };
  public static final  String        UTF_8    = "UTF-8";

  static {
    try {
      key = new SecretKeySpec(keyValue, AES);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static int checkDate(final String iLicense) throws OLicenseException {
    if (iLicense == null || iLicense.isEmpty())
      throw new OLicenseException("license not found");

    final String key = "@Ld" + "ks#2" + "" + "3dsLvc" + (35 - 12 * 2) + "a!Po" + "weRr";
    try {
      final Date now = new Date();

      final Date d = new SimpleDateFormat("yyyyMMdd").parse(OCry.decrypt(iLicense, key).substring(12));
      Calendar c = Calendar.getInstance();
      c.setTime(d);
      c.add(Calendar.DATE, DELAY);
      if (!c.getTime().after(now))
        throw new OLicenseException("license expired on: " + d);
      return getDateDiff(now, d);
    } catch (Exception e) {
      throw new OLicenseException("license not valid");
    }
  }

  public static int getClientId(final String iLicense) {
    final String key = "@Ld" + "ks#" + new Integer(27 - 4) + "dsLvc" + (13 - 4 + 2) + "a!Po" + "weRr";
    try {
      return Integer.parseInt(OCry.decrypt(iLicense, key).substring(0, 6));
    } catch (Exception e) {
      throw new RuntimeException("License not valid");
    }
  }

  public static int getServerId(final String iLicense) {
    final String key = "@Ld" + "ks#" + new Integer(23 + 9 - 19 + 10) + "dsLvc" + (110 / 10) + "a!Po" + "weRr";
    try {
      return Integer.parseInt(OCry.decrypt(iLicense, key).substring(6, 12));
    } catch (Exception e) {
      throw new RuntimeException("License not valid");
    }
  }

  private static int getDateDiff(Date date1, Date date2) {
    final long diffInMillies = date2.getTime() - date1.getTime();
    return (int) TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);
  }

  public static final class OCry {

    private static final int delta = 0x9E3779B9;

    public static String encrypt(final String iLicense, final String key) throws UnsupportedEncodingException {
      return new String(Base64.encodeBase64(encrypt(iLicense.getBytes(), key.getBytes())), UTF_8);
    }

    public static String decrypt(final String iLicense, final String key) throws IOException {
      return new String(decrypt(Base64.decodeBase64(iLicense), key.getBytes()));
    }

    public static final byte[] encrypt(byte[] data, byte[] key) {
      if (data.length == 0)
        return data;

      return toByteArray(encrypt(toIntArray(data, true), toIntArray(key, false)), false);
    }

    public static final byte[] decrypt(byte[] data, byte[] key) {
      if (data.length == 0)
        return data;

      return toByteArray(decrypt(toIntArray(data, false), toIntArray(key, false)), true);
    }

    private static final int[] encrypt(int[] v, int[] k) {
      int n = v.length - 1;

      if (n < 1) {
        return v;
      }
      if (k.length < 4) {
        int[] key = new int[4];

        System.arraycopy(k, 0, key, 0, k.length);
        k = key;
      }
      int z = v[n];
      int y = v[0];
      int sum = 0;
      int e;

      int p;
      int q = 6 + 52 / (n + 1);

      while (q-- > 0) {
        sum = sum + delta;
        e = sum >>> 2 & 3;
        for (p = 0; p < n; p++) {
          y = v[p + 1];
          z = v[p] += mx(sum, y, z, p, e, k);
        }
        y = v[0];
        z = v[n] += mx(sum, y, z, p, e, k);
      }
      return v;
    }

    private static final int[] decrypt(final int[] v, int[] k) {
      int n = v.length - 1;

      if (n < 1)
        return v;

      if (k.length < 4) {
        int[] key = new int[4];

        System.arraycopy(k, 0, key, 0, k.length);
        k = key;
      }
      int z = v[n];
      int y = v[0];
      int sum;
      int e;
      int p;
      int q = 6 + 52 / (n + 1);

      sum = q * delta;
      while (sum != 0) {
        e = sum >>> 2 & 3;
        for (p = n; p > 0; p--) {
          z = v[p - 1];
          y = v[p] -= mx(sum, y, z, p, e, k);
        }
        z = v[n];
        y = v[0] -= mx(sum, y, z, p, e, k);
        sum = sum - delta;
      }
      return v;
    }

    private static final int[] toIntArray(byte[] data, boolean includeLength) {
      int n = (((data.length & 3) == 0) ? (data.length >>> 2) : ((data.length >>> 2) + 1));
      final int[] result;

      if (includeLength) {
        result = new int[n + 1];
        result[n] = data.length;
      } else {
        result = new int[n];
      }
      n = data.length;
      for (int i = 0; i < n; i++) {
        result[i >>> 2] |= (0x000000ff & data[i]) << ((i & 3) << 3);
      }
      return result;
    }

    private static final byte[] toByteArray(final int[] data, final boolean includeLength) {
      int n = data.length << 2;

      if (includeLength) {
        int m = data[data.length - 1];

        if (m > n) {
          return null;
        } else {
          n = m;
        }
      }
      final byte[] result = new byte[n];

      for (int i = 0; i < n; i++)
        result[i] = (byte) ((data[i >>> 2] >>> ((i & 3) << 3)) & 0xff);

      return result;
    }

    private static final int mx(int sum, int y, int z, int p, int e, int[] k) {
      return (z >>> 5 ^ y << 2) + (y >>> 3 ^ z << 4) ^ (sum ^ y) + (k[p & 3 ^ e] ^ z);
    }
  }

  public static class OLicenseException extends Exception {
    private static final long serialVersionUID = 1L;

    public OLicenseException(String message) {
      super(message);
    }
  }

  public static String encrypt(String plainText) throws Exception {

    Cipher cipher = Cipher.getInstance(AES);
    cipher.init(Cipher.ENCRYPT_MODE, key);
    byte[] enc = cipher.doFinal(plainText.getBytes(UTF_8));
    String encryptedValue = Base64.encodeBase64String(enc);
    return encryptedValue;
  }

  public static String decrypt(String text) throws Exception {

    Cipher cipher = Cipher.getInstance(AES);
    cipher.init(Cipher.DECRYPT_MODE, key);
    byte[] decordedValue = Base64.decodeBase64(text);
    byte[] decValue = cipher.doFinal(decordedValue);
    String decryptedValue = new String(decValue, UTF_8);
    return decryptedValue;
  }

}
