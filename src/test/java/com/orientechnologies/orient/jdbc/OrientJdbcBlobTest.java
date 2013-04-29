package com.orientechnologies.orient.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

public class OrientJdbcBlobTest extends OrientJdbcBaseTest {

  @Test
  public void shouldLoadBlob() throws SQLException, FileNotFoundException, IOException, NoSuchAlgorithmException {
    File binaryFile = getOutFile();

    String digest = this.calculateMD5checksum(ClassLoader.getSystemResourceAsStream("file.pdf"));

    PreparedStatement stmt = conn.prepareStatement("SELECT FROM Article WHERE uuid = 1 ");

    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    rs.next();

    Blob blob = rs.getBlob("attachment");

    assertNotNull(blob);

    FileOutputStream s = null;
    try {
      s = new FileOutputStream(binaryFile);
      s.write(blob.getBytes(1, (int) blob.length()));
    } finally {
      if (s != null)
        s.close();
    }

    assertTrue("The file '" + binaryFile.getName() + "' does not exist", binaryFile.exists());
    this.verifyMD5checksum(binaryFile, digest);

  }

  @Test
  public void shouldLoadChunckedBlob() throws SQLException, FileNotFoundException, IOException, NoSuchAlgorithmException {
    File binaryFile = getOutFile();

    String digest = this.calculateMD5checksum(ClassLoader.getSystemResourceAsStream("file.pdf"));

    PreparedStatement stmt = conn.prepareStatement("SELECT FROM Article WHERE uuid = 2 ");

    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    rs.next();

    Blob blob = rs.getBlob("attachment");

    assertNotNull(blob);

    FileOutputStream s = null;
    try {
      s = new FileOutputStream(binaryFile);
      s.write(blob.getBytes(1, (int) blob.length()));
    } finally {
      if (s != null)
        s.close();
    }

    assertTrue("The file '" + binaryFile.getName() + "' does not exist", binaryFile.exists());
    this.verifyMD5checksum(binaryFile, digest);

  }

  private void verifyMD5checksum(File fileToBeChecked, String digest) {
    try {
      assertEquals("The MD5 checksum of the file '" + fileToBeChecked.getAbsolutePath() + "' does not match the given one.",
          digest, calculateMD5checksum(new FileInputStream(fileToBeChecked)));
    } catch (NoSuchAlgorithmException e) {
      fail(e.getMessage());
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  private String calculateMD5checksum(InputStream fileStream) throws NoSuchAlgorithmException, IOException {
    MessageDigest md = MessageDigest.getInstance("MD5");

    try {
      fileStream = new DigestInputStream(fileStream, md);
      while (fileStream.read() != -1)
        ;
    } finally {
      try {
        fileStream.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return new BigInteger(1, md.digest()).toString(16);
  }

  protected File getOutFile() {
    File binaryFile = new File("./target/working/output_blob.pdf");
    if (binaryFile.exists()) {
      do {
        binaryFile.delete();
      } while (binaryFile.exists());
    } else
      binaryFile.mkdirs();
    return binaryFile;
  }
}
