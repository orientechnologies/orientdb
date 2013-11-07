package com.orientechnologies.agent;

import java.io.IOException;

public final class OCry {

  private static final int delta = 0x9E3779B9;

  @SuppressWarnings("restriction")
  public static String encrypt(final String iLicense, final String key) {
    return new sun.misc.BASE64Encoder().encode(encrypt(iLicense.getBytes(), key.getBytes()));
  }

  @SuppressWarnings("restriction")
  public static String decrypt(final String iLicense, final String key) throws IOException {
    return new String(decrypt(new sun.misc.BASE64Decoder().decodeBuffer(iLicense), key.getBytes()));
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
    int z = v[n], y = v[0], sum = 0, e;
    int p, q = 6 + 52 / (n + 1);

    while (q-- > 0) {
      sum = sum + delta;
      e = sum >>> 2 & 3;
      for (p = 0; p < n; p++) {
        y = v[p + 1];
        z = v[p] += MX(sum, y, z, p, e, k);
      }
      y = v[0];
      z = v[n] += MX(sum, y, z, p, e, k);
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
    int z = v[n], y = v[0], sum, e;
    int p, q = 6 + 52 / (n + 1);

    sum = q * delta;
    while (sum != 0) {
      e = sum >>> 2 & 3;
      for (p = n; p > 0; p--) {
        z = v[p - 1];
        y = v[p] -= MX(sum, y, z, p, e, k);
      }
      z = v[n];
      y = v[0] -= MX(sum, y, z, p, e, k);
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

  private static final int MX(int sum, int y, int z, int p, int e, int[] k) {
    return (z >>> 5 ^ y << 2) + (y >>> 3 ^ z << 4) ^ (sum ^ y) + (k[p & 3 ^ e] ^ z);
  }
}