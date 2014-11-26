package com.orientechnologies.website;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.*;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * Created by enricorisa on 26/06/14.
 */
public class OLicenseMassiveCreator {

//  public static void main(final String args[]) throws IOException {
//
//    System.out
//        .println("OrientDB License Massive Creator. Copyright (c) 2009-2013 Orient Technologies LTD. All rights reserved. This sw can be used only by Orient Technologies dev team");
//    if (args.length < 3)
//      syntaxError();
//
//    generatAllLicense(args[0], args[1], args[2]);
//  }

  private static void generatAllLicense(String agentFile, String clientFile, String dstFolder) throws IOException {
    File agent = new File(agentFile);
    File client = new File(clientFile);
    File destFolder = new File(dstFolder);

    checkInput(agent, client, destFolder);

    File newAgentFolder = new File(destFolder.getAbsolutePath() + File.separator + agent.getName());

    if (newAgentFolder.exists()) {
      newAgentFolder.delete();
    }
    newAgentFolder.mkdir();

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    OIOUtils.copyStream(new FileInputStream(client), out, -1);
    ODocument doc = new ODocument().fromJSON(out.toString(), "noMap");

    Collection<ODocument> documents = doc.field("clients");
    for (ODocument c : documents) {
      generateSingleClient(c, agent, newAgentFolder);
    }
  }

  private static void checkInput(File agent, File client, File destFolder) {
    if (!agent.exists()) {
      System.err.println("Agent jar not found");
      System.exit(-1);
    }
    if (!client.exists()) {
      System.err.println("Client file  found");
      System.exit(-1);
    }

    if (!destFolder.exists()) {
      try {
        destFolder.mkdir();
      } catch (Exception e) {

        System.exit(-1);
        System.err.println("Could not create desintation folder  found");
      }
    }
  }

  private static void generateSingleClient(ODocument c, File agent, File agentFolder) {
    int clientId = c.field("clientId");
    int servers = c.field("servers");
    Date expiration = c.field("expiration");
    generateSingleServer(servers, clientId, expiration, agent, agentFolder);

  }

  private static void generateSingleServer(int servers, int clientId, Date expiration, File agent, File agentFolder) {

    Calendar cal = Calendar.getInstance();
    cal.setTime(expiration);
    int year = cal.get(Calendar.YEAR);
    int month = cal.get(Calendar.MONTH) + 1;
    int day = cal.get(Calendar.DAY_OF_MONTH);

    String license = OLicenseManagerConsole.generate(clientId, servers, year, month, day);

    try {
      ZipFile zipFile = new ZipFile(agent.getAbsolutePath());
      String[] splitted = agent.getName().split("-");
      String name = splitted[0] + "_" + clientId + "-" + splitted[1];
      final ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(agentFolder.getAbsolutePath() + File.separator + name));

      for (Enumeration e = zipFile.entries(); e.hasMoreElements();) {
        ZipEntry entryIn = (ZipEntry) e.nextElement();
        if (!entryIn.getName().equalsIgnoreCase("plugin.json")) {
          zos.putNextEntry(entryIn);
          InputStream is = zipFile.getInputStream(entryIn);
          byte[] buf = new byte[1024];
          int len;
          while ((len = (is.read(buf))) > 0) {
            zos.write(buf, 0, len);
          }
        } else {
          zos.putNextEntry(new ZipEntry("plugin.json"));
          InputStream is = zipFile.getInputStream(entryIn);

          InputStreamReader isr = new InputStreamReader(is);
          StringBuilder sb = new StringBuilder();
          BufferedReader br = new BufferedReader(isr);
          String read = br.readLine();

          while (read != null) {
            sb.append(read);
            read = br.readLine();

          }
          zos.write(sb.toString().replace("@LICENSE@", license).getBytes());

        }
        zos.closeEntry();
      }
      zos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void syntaxError() {
    System.err.println("Syntax error, usage:");
    System.err.println("> licensecreator <agent.*.jar> <client.json> <destFolder>");
  }
}
