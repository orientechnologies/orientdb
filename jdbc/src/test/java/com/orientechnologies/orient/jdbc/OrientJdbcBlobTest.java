package com.orientechnologies.orient.jdbc;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.*;
import java.math.BigInteger;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class OrientJdbcBlobTest extends OrientJdbcBaseTest {
  private static final String TEST_WORKING_DIR = "./target/working/";

  @Test
  public void shouldStoreBinaryStream() throws Exception {
    conn.createStatement().executeQuery("CREATE CLASS Blobs");

    PreparedStatement statement = conn.prepareStatement("INSERT INTO Blobs (uuid,attachment) VALUES (?,?)");

    statement.setInt(1, 1);
    statement.setBinaryStream(2, ClassLoader.getSystemResourceAsStream("file.pdf"));

    int rowsInserted = statement.executeUpdate();

    assertThat(rowsInserted, Matchers.is(1));

    //verify the blob

    PreparedStatement stmt = conn.prepareStatement("SELECT FROM Blobs WHERE uuid = 1 ");

    ResultSet rs = stmt.executeQuery();
    assertThat(rs.next(), Matchers.is(true));
    rs.next();

    Blob blob = rs.getBlob("attachment");
    verifyBlobAgainstFile(blob);

  }

  private void verifyBlobAgainstFile(Blob blob) throws NoSuchAlgorithmException, IOException, SQLException {
    String digest = this.calculateMD5checksum(ClassLoader.getSystemResourceAsStream("file.pdf"));
    File binaryFile = getOutFile();

    assertThat(blob, Matchers.notNullValue());

    dumpBlobToFile(binaryFile, blob);

    assertThat(binaryFile.exists(), Matchers.is(true));

    verifyMD5checksum(binaryFile, digest);

  }

  @Test
  public void shouldLoadBlob() throws SQLException, IOException, NoSuchAlgorithmException {

    PreparedStatement stmt = conn.prepareStatement("SELECT FROM Article WHERE uuid = 1 ");

    ResultSet rs = stmt.executeQuery();
    assertThat(rs.next(), Matchers.is(true));
    rs.next();

    Blob blob = rs.getBlob("attachment");

    verifyBlobAgainstFile(blob);

  }

  @Test
  public void shouldLoadChuckedBlob() throws SQLException, IOException, NoSuchAlgorithmException {

    PreparedStatement stmt = conn.prepareStatement("SELECT FROM Article WHERE uuid = 2 ");

    ResultSet rs = stmt.executeQuery();
    assertThat(rs.next(), Matchers.is(true));
    rs.next();

    Blob blob = rs.getBlob("attachment");

    verifyBlobAgainstFile(blob);

  }

  protected void createWorkingDirIfRequired() {
    new File(TEST_WORKING_DIR).mkdirs();
  }

  protected File getOutFile() {
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
      assertEquals("The MD5 checksum of the file '" + fileToBeChecked.getAbsolutePath() + "' does not match the given one.", digest,
          calculateMD5checksum(new FileInputStream(fileToBeChecked)));
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
