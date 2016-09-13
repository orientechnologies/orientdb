package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OStorage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.util.List;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.*;
import static org.assertj.core.api.Assertions.*;

/**
 * Created by frank on 18/07/2016.
 */
public class OrientGraphFactoryEncryptionTest {

  @Rule
  public TestName name = new TestName();

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private String dbPath;

  @Before
  public void setUp() throws Exception {
    dbPath = folder.newFolder(name.getMethodName()).getAbsolutePath();

  }

  @Test
  public void testCreatedAESEncryptedCluster() {

    OrientGraphFactory graphFactory = new OrientGraphFactory("plocal:" + dbPath);

    graphFactory.setProperty(STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");

    ODatabaseDocumentInternal db = graphFactory.getDatabase();

    assertThat(db.getProperty(STORAGE_ENCRYPTION_KEY.getKey())).isEqualTo("T1JJRU5UREJfSVNfQ09PTA==");
    db.close();

    db = graphFactory.getNoTx().getDatabase();

    assertThat(db.getProperty(STORAGE_ENCRYPTION_KEY.getKey())).isEqualTo("T1JJRU5UREJfSVNfQ09PTA==");
    db.close();

    db = graphFactory.getNoTx().getRawGraph();

    assertThat(db.getProperty(STORAGE_ENCRYPTION_KEY.getKey())).isEqualTo("T1JJRU5UREJfSVNfQ09PTA==");
    db.close();
    
    graphFactory.close();

  }

  @Test
  public void shouldQueryDESEncryptedDatabase() {

    OrientGraphFactory graphFactory = new OrientGraphFactory("plocal:" + dbPath);

    graphFactory.setProperty(STORAGE_ENCRYPTION_METHOD.getKey(), "des");
    graphFactory.setProperty(STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");

    ODatabaseDocumentTx db = graphFactory.getDatabase();

    db.command(new OCommandSQL("create class TestEncryption")).execute();
    db.command(new OCommandSQL("insert into TestEncryption set name = 'Jay'")).execute();

    List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));

    assertThat(result).hasSize(1);
    db.close();

    graphFactory.close();
    
  }

  @Test
  public void shouldFailWitWrongKey() {

    ODatabase db = new ODatabaseDocumentTx("plocal:" + dbPath);

    db.setProperty(STORAGE_ENCRYPTION_METHOD.getKey(), "des");
    db.setProperty(STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");

    db.create();

    db.close();

    OStorage storage = ((ODatabaseDocumentInternal) db).getStorage();

    storage.close();

    OrientGraphFactory graphFactory = new OrientGraphFactory("plocal:" + dbPath);

    graphFactory.setProperty(STORAGE_ENCRYPTION_METHOD.getKey(), "des");
    graphFactory.setProperty(STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");

    db = graphFactory.getDatabase();

    db.command(new OCommandSQL("create class TestEncryption")).execute();
    db.command(new OCommandSQL("insert into TestEncryption set name = 'Jay'")).execute();

    db.close();
    storage = ((ODatabaseDocumentInternal) db).getStorage();

    graphFactory.close();

    storage.close();
    //    Orient.instance().shutdown();
    //    Orient.instance().startup();

    graphFactory = new OrientGraphFactory("plocal:" + dbPath);

    graphFactory.setProperty(STORAGE_ENCRYPTION_METHOD.getKey(), "des");
    graphFactory.setProperty(STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");

    db = graphFactory.getDatabase();
    List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));

    assertThat(result).hasSize(1);
    db.close();
    graphFactory.close();

  }

  public void verifyDatabaseEncryption(OrientGraphFactory fc) {
    ODatabaseDocumentTx db = fc.getDatabase();
    db.command(new OCommandSQL("create class TestEncryption")).execute();
    db.command(new OCommandSQL("insert into TestEncryption set name = 'Jay'")).execute();

    List result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
    Assert.assertEquals(result.size(), 1);
    db.close();

    db = fc.getDatabase();
    OStorage storage = ((ODatabaseDocumentInternal) db).getStorage();
    db.close();

    storage.close(true, false);

    fc.setProperty(STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");
    db = fc.getDatabase();

    result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
    Assert.assertEquals(result.size(), 1);
    storage = ((ODatabaseDocumentInternal) db).getStorage();
    db.close();

    storage.close(true, false);

    db = fc.getDatabase();
    db.setProperty(STORAGE_ENCRYPTION_KEY.getKey(), "invalidPassword");
    try {
      storage = ((ODatabaseDocumentInternal) db).getStorage();
      Assert.fail();
    } catch (OSecurityException e) {
      Assert.assertTrue(true);
    } finally {
      db.activateOnCurrentThread();
      db.close();
      storage.close(true, false);
    }

    fc.setProperty(STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA=-");
    try {
      db = fc.getDatabase();
      storage = ((ODatabaseDocumentInternal) db).getStorage();
      Assert.fail();
    } catch (OSecurityException e) {
      Assert.assertTrue(true);
    } finally {
      db.activateOnCurrentThread();
      db.close();
      storage.close(true, false);
    }

    fc.setProperty(STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");
    db = fc.getDatabase();
    result = db.query(new OSQLSynchQuery<ODocument>("select from TestEncryption"));
    Assert.assertEquals(result.size(), 1);
    db.close();

  }

  @Test
  public void testCreatedDESEncryptedCluster() {

    OrientGraphFactory graphFactory = new OrientGraphFactory("plocal:" + dbPath);

    graphFactory.setProperty(STORAGE_ENCRYPTION_KEY.getKey(), "T1JJRU5UREJfSVNfQ09PTA==");

    ODatabaseDocumentTx db = graphFactory.getDatabase();
    //    verifyClusterEncryption(db, "des");
    db.close();
    graphFactory.close();
  }

}
