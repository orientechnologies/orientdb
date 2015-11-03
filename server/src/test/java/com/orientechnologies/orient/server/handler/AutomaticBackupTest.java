package com.orientechnologies.orient.server.handler;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Luca Garulli
 */
public class AutomaticBackupTest {
  private final static String DBNAME    = "testautobackup";
  private final static String BACKUPDIR = "target/backup";
  private static final String URL       = "plocal:target/" + DBNAME;
  private static final String URL2      = "plocal:target/" + DBNAME + "2";
  private ODatabaseDocumentTx database;
  private final OServer       server;

  public AutomaticBackupTest() throws ClassNotFoundException, MalformedObjectNameException, InstanceAlreadyExistsException,
      NotCompliantMBeanException, MBeanRegistrationException {
    server = new OServer() {
      @Override
      public Map<String, String> getAvailableStorageNames() {
        HashMap<String, String> result = new HashMap<String, String>();
        result.put(DBNAME, URL);
        return result;
      }
    };
  }

  @BeforeClass
  @AfterClass
  public static final void clean() {
    OFileUtils.deleteRecursively(new File(BACKUPDIR));
  }

  @Before
  public void init() {
    database = new ODatabaseDocumentTx(URL);
    if (database.exists())
      database.open("admin", "admin").drop();

    database.create();

    new ODocument("TestBackup").field("name", DBNAME).save();
  }

  @After
  public void deinit() {
    database.activateOnCurrentThread();
    database.drop();
  }

  @Test
  public void testFullBackup() throws IOException, ClassNotFoundException, MalformedObjectNameException,
      InstanceAlreadyExistsException, NotCompliantMBeanException, MBeanRegistrationException {
    final OAutomaticBackup aBackup = new OAutomaticBackup();

    final OServerParameterConfiguration[] config = new OServerParameterConfiguration[] {
        new OServerParameterConfiguration("firstTime",
            new SimpleDateFormat("HH:mm:ss").format(new Date(System.currentTimeMillis() + 2000))),
        new OServerParameterConfiguration("delay", "1d"), new OServerParameterConfiguration("mode", "FULL_BACKUP"),
        new OServerParameterConfiguration("target.directory", BACKUPDIR),
        new OServerParameterConfiguration("target.fileName", "fullBackup.zip") };

    aBackup.config(server, config);

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    aBackup.sendShutdown();

    final ODatabaseDocumentTx database2 = new ODatabaseDocumentTx(URL2);
    if (database2.exists())
      database2.open("admin", "admin").drop();
    database2.create();

    database2.restore(new FileInputStream(BACKUPDIR + "/fullBackup.zip"), null, null, null);

    Assert.assertEquals(database2.countClass("TestBackup"), 1);
  }

  @Test
  public void testIncrementalBackup() throws IOException, ClassNotFoundException, MalformedObjectNameException,
      InstanceAlreadyExistsException, NotCompliantMBeanException, MBeanRegistrationException {
    final OAutomaticBackup aBackup = new OAutomaticBackup();

    final OServerParameterConfiguration[] config = new OServerParameterConfiguration[] {
        new OServerParameterConfiguration("firstTime",
            new SimpleDateFormat("HH:mm:ss").format(new Date(System.currentTimeMillis() + 2000))),
        new OServerParameterConfiguration("delay", "1d"), new OServerParameterConfiguration("mode", "INCREMENTAL_BACKUP"),
        new OServerParameterConfiguration("target.directory", BACKUPDIR) };

    aBackup.config(server, config);

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    aBackup.sendShutdown();

    final ODatabaseDocumentTx database2 = new ODatabaseDocumentTx(URL2);
    if (database2.exists())
      database2.open("admin", "admin").drop();
    database2.create();

    database2.incrementalRestore(BACKUPDIR + "/" + DBNAME);

    Assert.assertEquals(database2.countClass("TestBackup"), 1);
  }

  @Test
  public void testExport() throws IOException, ClassNotFoundException, MalformedObjectNameException, InstanceAlreadyExistsException,
      NotCompliantMBeanException, MBeanRegistrationException {
    final OAutomaticBackup aBackup = new OAutomaticBackup();

    final OServerParameterConfiguration[] config = new OServerParameterConfiguration[] {
        new OServerParameterConfiguration("firstTime",
            new SimpleDateFormat("HH:mm:ss").format(new Date(System.currentTimeMillis() + 2000))),
        new OServerParameterConfiguration("delay", "1d"), new OServerParameterConfiguration("mode", "EXPORT"),
        new OServerParameterConfiguration("target.directory", BACKUPDIR),
        new OServerParameterConfiguration("target.fileName", "fullBackup.json.gz") };

    aBackup.config(server, config);

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    aBackup.sendShutdown();

    final ODatabaseDocumentTx database2 = new ODatabaseDocumentTx(URL2);
    if (database2.exists())
      database2.open("admin", "admin").drop();
    database2.create();

    new ODatabaseImport(database2, BACKUPDIR + "/" + "fullBackup.json.gz", null).importDatabase();

    Assert.assertEquals(database2.countClass("TestBackup"), 1);
  }
}
