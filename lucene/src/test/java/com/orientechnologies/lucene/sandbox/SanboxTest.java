package com.orientechnologies.lucene.sandbox;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

import java.io.File;

/**
 * Created by frank on 03/03/2016.
 */
public class SanboxTest {

  @org.junit.Test
  public void testName() throws Exception {

    OFileUtils.deleteRecursively(new File("./target/testDatabase"));

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:./target/testDatabase");
    db.create();
    //    db.open("admin","admin");
    OStorage storage = db.getStorage();
    OLocalPaginatedStorage underlying = (OLocalPaginatedStorage) db.getStorage().getUnderlying();
    String databaseName = OIOUtils.getPathFromDatabaseName(storage.getURL());
    System.out.println("url:: " + storage.getURL());
    System.out.println("name:: " + databaseName);

    System.out.println("dir:: " + storage.getConfiguration().getDirectory());

    
    System.out.println("pag dir:: " + OIOUtils.getPathFromDatabaseName(underlying.getURL()));
    System.out.println("pag storagepath:: " + underlying.getStoragePath());

  }
}
