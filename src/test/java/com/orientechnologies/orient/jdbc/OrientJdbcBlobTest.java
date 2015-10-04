package com.orientechnologies.orient.jdbc;

import org.hamcrest.Matchers;
import org.hamcrest.core.Is;
import org.junit.Test;

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

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class OrientJdbcBlobTest extends OrientJdbcBaseTest {
  private static final String TEST_WORKING_DIR = "./target/working/";

  @Test
  public void shouldLoadBlob() throws SQLException, FileNotFoundException, IOException, NoSuchAlgorithmException {
    File binaryFile = getOutFile();

    String digest = this.calculateMD5checksum(ClassLoader.getSystemResourceAsStream("file.pdf"));

    PreparedStatement stmt = conn.prepareStatement("SELECT FROM Article WHERE uuid = 1 ");

    ResultSet rs = stmt.executeQuery();
    assertThat(rs.next(), is(true));
    rs.next();

    Blob blob = rs.getBlob("attachment");

    assertThat(blob, notNullValue());

    dumpBlobToFile(binaryFile, blob);

    assertTrue("The file '" + binaryFile.getName() + "' does not exist", binaryFile.exists());
    verifyMD5checksum(binaryFile, digest);

  }


  @Test
  public void shouldLoadChuckedBlob() throws SQLException, FileNotFoundException, IOException, NoSuchAlgorithmException {
    File binaryFile = getOutFile();

    String digest = this.calculateMD5checksum(ClassLoader.getSystemResourceAsStream("file.pdf"));

    PreparedStatement stmt = conn.prepareStatement("SELECT FROM Article WHERE uuid = 2 ");

    ResultSet rs = stmt.executeQuery();
    assertThat(rs.next(), is(true));
    rs.next();

    Blob blob = rs.getBlob("attachment");

    assertThat(blob, notNullValue());

    dumpBlobToFile(binaryFile, blob);

    assertTrue("The file '" + binaryFile.getName() + "' does not exist", binaryFile.exists());
    this.verifyMD5checksum(binaryFile, digest);

  }

  protected void createWorkingDirIfRequired() {
    new File(TEST_WORKING_DIR).mkdirs();
  }

  protected File getOutFile() {
    File binaryFile = new File("./target/working/output_blob.pdf");
    createWorkingDirIfRequired();
    File outFile = new File(TEST_WORKING_DIR + "output_blob.pdf");
    deleteFileIfItExists(outFile);
    return outFile;
  }

  protected void deleteFileIfItExists(File file) {
    if (file.exists()) {
      do {
        file.delete();
      } while (file.exists());
    }
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

  private void dumpBlobToFile(File binaryFile, Blob blob) throws IOException, SQLException {
    FileOutputStream s = null;
    try {
      s = new FileOutputStream(binaryFile);
      s.write(blob.getBytes(1, (int) blob.length()));
    } finally {
      if (s != null)
        s.close();
    }
  }

}
