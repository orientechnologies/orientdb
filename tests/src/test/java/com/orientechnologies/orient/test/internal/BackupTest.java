package com.orientechnologies.orient.test.internal;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.zip.ZipFile;

/** Created by luca on 03/04/14. */
public class BackupTest {
  public static void main(String[] args) throws IOException {
    File backupFile = new File("testbackup.zip");
    OutputStream oS = new FileOutputStream(backupFile);

    OrientGraphFactory factory = new OrientGraphFactory("plocal:backupTest", "admin", "admin");
    OrientGraphNoTx graphNoTx = factory.getNoTx();
    graphNoTx.getRawGraph().backup(oS, null, null, null, 1, 1024);

    ZipFile zipFile = new ZipFile(backupFile);
    Enumeration enumeration = zipFile.entries();
    System.out.format("ZipFile : %s%n", zipFile);
    while (enumeration.hasMoreElements()) {
      Object entry = enumeration.nextElement();
      System.out.format("  Entry : %s%n", entry);
    }
    factory.close();
  }
}
