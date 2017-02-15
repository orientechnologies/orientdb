package com.orientechnologies.orient.server.handler;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
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
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class AutomaticBackupTest {
  private final static String DBNAME    = "testautobackup";
  private final static String DBNAME2    = DBNAME + "2";
  private final static String BACKUPDIR = "target/backup";
  private static final String URL       = "plocal:target/" + DBNAME;
  private static final String URL2      = "plocal:target/" + DBNAME + "2";
  private final String        tempDirectory;
  private ODatabaseDocument   database;
  private final OServer       server;

  public AutomaticBackupTest() throws ClassNotFoundException, MalformedObjectNameException, InstanceAlreadyExistsException,
      NotCompliantMBeanException, MBeanRegistrationException, IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException {

    // SET THE ORIENTDB_HOME DIRECTORY TO CHECK JSON FILE CREATION
    tempDirectory = new File("target/testhome").getAbsolutePath();
    System.setProperty("ORIENTDB_HOME", tempDirectory);

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
  public void init() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException {
    final File f = new File(OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/config/automatic-backup.json"));
    if (f.exists())
      f.delete();
    server.startup();
    if (server.existsDatabase(DBNAME))
      server.dropDatabase(DBNAME);
    server.createDatabase(DBNAME, ODatabaseType.PLOCAL, null);
    database = server.openDatabase(DBNAME, null, null, null, true);

    new ODocument("TestBackup").field("name", DBNAME).save();
  }

  @After
  public void deinit() {
    Assert.assertTrue(new File(tempDirectory + "/config/automatic-backup.json").exists());

    new File(tempDirectory + "/config/automatic-backup.json").delete();

    server.dropDatabase(database.getName());
    server.shutdown();

  }

  @Test
  public void testFullBackupWithJsonConfigInclude() throws IOException, ClassNotFoundException, MalformedObjectNameException,
      InstanceAlreadyExistsException, NotCompliantMBeanException, MBeanRegistrationException {
    if (new File(BACKUPDIR + "/testautobackup.zip").exists())
      new File(BACKUPDIR + "/testautobackup.zip").delete();

    Assert.assertFalse(new File(tempDirectory + "/config/automatic-backup.json").exists());

    String jsonConfig = OIOUtils.readStreamAsString(getClass().getResourceAsStream("automatic-backup.json"));

    ODocument doc = new ODocument().fromJSON(jsonConfig);

    doc.field("enabled", true);
    doc.field("targetFileName", "${DBNAME}.zip");

    doc.field("dbInclude", new String[] { "testautobackup" });

    doc.field("firstTime", new SimpleDateFormat("HH:mm:ss").format(new Date(System.currentTimeMillis() + 2000)));

    OIOUtils.writeFile(new File(tempDirectory + "/config/automatic-backup.json"), doc.toJSON());

    final OAutomaticBackup aBackup = new OAutomaticBackup();

    final OServerParameterConfiguration[] config = new OServerParameterConfiguration[] {};

    aBackup.config(server, config);

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    aBackup.sendShutdown();

    if (server.existsDatabase(DBNAME2))
      server.dropDatabase(DBNAME2);
    server.createDatabase(DBNAME2, ODatabaseType.PLOCAL, null);
    ODatabaseDocument database2 = server.openDatabase(DBNAME2, null, null, null, true);

    database2.restore(new FileInputStream(BACKUPDIR + "/testautobackup.zip"), null, null, null);

    Assert.assertEquals(database2.countClass("TestBackup"), 1);
    database2.close();
  }

  @Test
  public void testFullBackupWithJsonConfigExclude() throws IOException, ClassNotFoundException, MalformedObjectNameException,
      InstanceAlreadyExistsException, NotCompliantMBeanException, MBeanRegistrationException {
    if (new File(BACKUPDIR + "/testautobackup.zip").exists())
      new File(BACKUPDIR + "/testautobackup.zip").delete();

    Assert.assertFalse(new File(tempDirectory + "/config/automatic-backup.json").exists());

    String jsonConfig = OIOUtils.readStreamAsString(getClass().getResourceAsStream("automatic-backup.json"));

    ODocument doc = new ODocument().fromJSON(jsonConfig);

    doc.field("enabled", true);
    doc.field("targetFileName", "${DBNAME}.zip");

    doc.field("dbExclude", new String[] { "testautobackup" });

    doc.field("firstTime", new SimpleDateFormat("HH:mm:ss").format(new Date(System.currentTimeMillis() + 2000)));

    OIOUtils.writeFile(new File(tempDirectory + "/config/automatic-backup.json"), doc.toJSON());

    final OAutomaticBackup aBackup = new OAutomaticBackup();

    final OServerParameterConfiguration[] config = new OServerParameterConfiguration[] {};

    aBackup.config(server, config);

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    aBackup.sendShutdown();

    Assert.assertFalse(new File(BACKUPDIR + "/testautobackup.zip").exists());
  }

  @Test
  public void testFullBackup() throws IOException, ClassNotFoundException, MalformedObjectNameException,
      InstanceAlreadyExistsException, NotCompliantMBeanException, MBeanRegistrationException {
    if (new File(BACKUPDIR + "/fullBackup.zip").exists())
      new File(BACKUPDIR + "/fullBackup.zip").delete();

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
    
    database2.close();
  }

  //// @Test
  // //TODO: move to EE test suite
  // public void testIncrementalBackup() throws IOException, ClassNotFoundException, MalformedObjectNameException,
  // InstanceAlreadyExistsException, NotCompliantMBeanException, MBeanRegistrationException {
  // if (new File(BACKUPDIR + "/" + DBNAME).exists())
  // OFileUtils.deleteRecursively(new File(BACKUPDIR + "/" + DBNAME));
  //
  // final OAutomaticBackup aBackup = new OAutomaticBackup();
  //
  // final OServerParameterConfiguration[] config = new OServerParameterConfiguration[] {
  // new OServerParameterConfiguration("firstTime",
  // new SimpleDateFormat("HH:mm:ss").format(new Date(System.currentTimeMillis() + 2000))),
  // new OServerParameterConfiguration("delay", "1d"), new OServerParameterConfiguration("mode", "INCREMENTAL_BACKUP"),
  // new OServerParameterConfiguration("target.directory", BACKUPDIR) };
  //
  // aBackup.config(server, config);
  //
  // try {
  // Thread.sleep(5000);
  // } catch (InterruptedException e) {
  // e.printStackTrace();
  // }
  //
  // aBackup.sendShutdown();
  //
  // final ODatabaseDocumentTx database2 = new ODatabaseDocumentTx(URL2);
  // if (database2.exists())
  // database2.open("admin", "admin").drop();
  // database2.create(BACKUPDIR + "/" + DBNAME);
  //
  // Assert.assertEquals(database2.countClass("TestBackup"), 1);
  // }

  @Test
  public void testExport() throws IOException, ClassNotFoundException, MalformedObjectNameException, InstanceAlreadyExistsException,
      NotCompliantMBeanException, MBeanRegistrationException {
    if (new File(BACKUPDIR + "/fullExport.json.gz").exists())
      new File(BACKUPDIR + "/fullExport.json.gz").delete();

    final OAutomaticBackup aBackup = new OAutomaticBackup();

    final OServerParameterConfiguration[] config = new OServerParameterConfiguration[] {
        new OServerParameterConfiguration("firstTime",
            new SimpleDateFormat("HH:mm:ss").format(new Date(System.currentTimeMillis() + 2000))),
        new OServerParameterConfiguration("delay", "1d"), new OServerParameterConfiguration("mode", "EXPORT"),
        new OServerParameterConfiguration("target.directory", BACKUPDIR),
        new OServerParameterConfiguration("target.fileName", "fullExport.json.gz") };

    aBackup.config(server, config);

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    aBackup.sendShutdown();

    if (server.existsDatabase(DBNAME2))
      server.dropDatabase(DBNAME2);
    server.createDatabase(DBNAME2, ODatabaseType.PLOCAL, null);
    
    ODatabaseDocumentInternal database2 = server.openDatabase(DBNAME2, null, null, null, true); 
    

    new ODatabaseImport(database2, BACKUPDIR + "/fullExport.json.gz", null).importDatabase();

    Assert.assertEquals(database2.countClass("TestBackup"), 1);
    
    database2.close();
  }
}
