package com.orientechnologies.backup.uploader;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.jcraft.jsch.*;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Vector;
import org.junit.*;

public class OSFTPDeltaUploadingStrategyTest {
  private static final ODocument cfg = new ODocument();
  private static OrientDB orientDB;
  private static final String dbName = "upload-sftp";
  private static String backupLocation;
  private static final String backupPath = "target/backup/";

  @Test
  public void testFileUpload() throws JSchException, SftpException {
    JSch jsch = mock(JSch.class);
    Session session = mock(Session.class);
    ChannelSftp channelSftp = mock(ChannelSftp.class);
    when(jsch.getSession(anyString(), anyString(), anyInt())).thenReturn(session);
    when(session.openChannel("sftp")).thenReturn(channelSftp);

    OSFTPDeltaUploadingStrategy strategy = new OSFTPDeltaUploadingStrategy();
    strategy.config(cfg);
    OUploadMetadata metadata =
        strategy.executeSftpFileUpload(jsch, backupLocation, "file.backup", dbName);
    assertNotNull(metadata);

    verify(session).connect();
    verify(session).openChannel("sftp");
    verify(channelSftp).connect();
    verify(channelSftp).put(any(InputStream.class), eq("file.backup"));
  }

  @Test
  public void testBackupUploadDownload() throws JSchException, SftpException {
    JSch jsch = mock(JSch.class);
    Session session = mock(Session.class);
    ChannelSftp channelSftp = mock(ChannelSftp.class);
    when(jsch.getSession(anyString(), anyString(), anyInt())).thenReturn(session);
    when(session.openChannel("sftp")).thenReturn(channelSftp);
    when(channelSftp.ls("*")).thenReturn(new Vector());

    OSFTPDeltaUploadingStrategy strategy = new OSFTPDeltaUploadingStrategy();
    strategy.config(cfg);
    boolean success = strategy.executeSftpUpload(jsch, backupPath, "sftp-backup");
    assertTrue(success);

    verify(session).connect();
    verify(session).openChannel("sftp");
    verify(channelSftp).connect();
    verify(channelSftp).put(any(InputStream.class), anyString());
    verify(channelSftp).disconnect();
    verify(session).disconnect();
  }

  @BeforeClass
  public static void setup() {
    cfg.setProperty("username", "admin");
    cfg.setProperty("host", "localhost");
    cfg.setProperty("port", 22);
    cfg.setProperty("path", "/backup");
    // Create DB and schema
    String dbURL = "plocal:target/upload-sftp";
    orientDB = new OrientDB(dbURL, OrientDBConfig.defaultConfig());
    orientDB.create(dbName, ODatabaseType.PLOCAL);
    // TODO: create admin user
    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      /* Create schema */
      OClass personClass = session.createVertexClass("Person");
      personClass.createProperty("id", OType.STRING);
      int count = 10;
      for (int i = 0; i < count; i++) {
        final int vertexId = i;
        int maxRetries = 5;
        session.executeWithRetry(
            maxRetries,
            oDatabaseSession -> {
              OVertex v = session.newVertex("Person");
              v.setProperty("id", vertexId);
              v.save();
              return null;
            });
      }
      String fileBackupName = session.incrementalBackup(backupPath);
      backupLocation = Paths.get(backupPath, fileBackupName).toString();
    }
  }

  @AfterClass
  public static void tearDown() {
    orientDB.drop(dbName);
    orientDB.close();
    OFileUtils.deleteRecursively(new File(backupPath));
  }
}
