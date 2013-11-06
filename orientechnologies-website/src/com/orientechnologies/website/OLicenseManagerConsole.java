package com.orientechnologies.website;

import java.io.IOException;

import sun.misc.BASE64Encoder;

@SuppressWarnings("restriction")
public class OLicenseManagerConsole {

  public static void main(final String args[]) throws IOException {
    System.out
        .println("OrientDB License Manager. Copyright (c) 2009-2013 Orient Technologies LTD. All rights reserved. This sw can be used only by Orient Technologies dev team");

    if (args.length < 3)
      syntaxError();
    else {
      int clientId = Integer.parseInt(args[0]);
      int serverId = Integer.parseInt(args[1]);
      int year = Integer.parseInt(args[2].substring(0, 4));
      int month = Integer.parseInt(args[2].substring(4, 6));
      int day = Integer.parseInt(args[2].substring(6, 8));

      System.out.format("\n- Client id...: %d", clientId);
      System.out.format("\n- Server id...: %d", serverId);
      System.out.format("\n- Expiration..: %d-%d-%d (yyyy-MM-dd)", year, month, day);

      final String license = generate(clientId, serverId, year, month, day);
      System.out.format("\n- License.....: %s", license);
      System.out.format("\n\n", license);
    }

  }

  public static String generate(final int clientId, int serverId, int year, int month, int day) {
    String plaintext = String.format("%06d%06d%04d%02d%02d", clientId, serverId, year, month, day);

    final String key = "@Ld" + "ks#2" + new Integer(7 - 4) + "dsLvc" + (35 - 18 - 6) + "a!Po" + "weRr";

    final String license = en(plaintext, key);
    if (license.endsWith("\n"))
      return license.substring(0, license.length() - 1);
    return license;
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

  private static void syntaxError() {
    System.err.println("Syntax error, usage:");
    System.err.println("> licensemgr <clientId> <serverId> <expiration yyyy-MM-dd>");
  }
}
