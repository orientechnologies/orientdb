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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

@SuppressWarnings("restriction")
public class OL {

  public static String generate(final int clientId, int serverId, int year, int month, int day) {
    String plaintext = String.format("%06d%06d%04d%02d%02d", clientId, serverId, year, month, day);

    final String key = "@Ld" + "ks#2" + new Integer(7 - 4) + "dsLvc" + (35 - 18 - 6) + "a!Po" + "weRr";

    final String license = en(plaintext, key);
    if (license.endsWith("\n"))
      return license.substring(0, license.length() - 1);
    return license;
  }

  public static int checkDate(final String iLicense) {
    final String key = "@Ld" + "ks#2" + "" + "3dsLvc" + (35 - 12 * 2) + "a!Po" + "weRr";
    try {
      final Date now = new Date();
      final Date d = new SimpleDateFormat("yyyyMMdd").parse(de(iLicense, key).substring(12));
      if (!d.after(now))
        throw new RuntimeException("License expired on: " + d);
      return getDateDiff(d, now);
    } catch (Exception e) {
      throw new RuntimeException("License not valid");
    }
  }

  public static int getClientId(final String iLicense) {
    final String key = "@Ld" + "ks#" + new Integer(27 - 4) + "dsLvc" + (13 - 4 + 2) + "a!Po" + "weRr";
    try {
      return Integer.parseInt(de(iLicense, key).substring(0, 6));
    } catch (Exception e) {
      throw new RuntimeException("License not valid");
    }
  }

  public static int getServerId(final String iLicense) {
    final String key = "@Ld" + "ks#" + new Integer(23 + 9 - 19 + 10) + "dsLvc" + (110 / 10) + "a!Po" + "weRr";
    try {
      return Integer.parseInt(de(iLicense, key).substring(6, 12));
    } catch (Exception e) {
      throw new RuntimeException("License not valid");
    }
  }

  private static String en(final String message, final String key) {
    if (message == null || key == null)
      return null;

    char[] keys = key.toCharArray();
    char[] mesg = message.toCharArray();
    final BASE64Encoder encoder = new BASE64Encoder();

    int ml = mesg.length;
    int kl = keys.length;
    char[] newmsg = new char[ml];

    for (int i = 0; i < ml; i++) {
      newmsg[i] = (char) (mesg[i] ^ keys[i % kl]);
    }
    mesg = null;
    keys = null;
    String temp = new String(newmsg);
    return new String(encoder.encodeBuffer(temp.getBytes()));
  }

  private static String de(String message, final String key) throws IOException {
    if (message == null || key == null)
      return null;
    final BASE64Decoder decoder = new BASE64Decoder();
    char[] keys = key.toCharArray();
    message = new String(decoder.decodeBuffer(message));
    char[] mesg = message.toCharArray();

    int ml = mesg.length;
    int kl = keys.length;
    final char[] newmsg = new char[ml];

    for (int i = 0; i < ml; i++) {
      newmsg[i] = (char) (mesg[i] ^ keys[i % kl]);
    }
    mesg = null;
    keys = null;
    return new String(newmsg);
  }

  private static int getDateDiff(Date date1, Date date2) {
    final long diffInMillies = date2.getTime() - date1.getTime();
    return (int) TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);
  }
}
