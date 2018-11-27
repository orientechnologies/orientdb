package com.orientechnologies.orient.core.storage.cluster.v0;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.storage.cluster.LocalPaginatedClusterAbstract;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 26.03.13
 */

public class LocalPaginatedClusterV0TestIT extends LocalPaginatedClusterAbstract {
  @BeforeClass
  public static void beforeClass() throws IOException {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null || buildDirectory.isEmpty())
      buildDirectory = ".";

    buildDirectory += "/localPaginatedClusterTest";

    databaseDocumentTx = new ODatabaseDocumentTx(
        "plocal:" + buildDirectory + File.separator + LocalPaginatedClusterV0TestIT.class.getSimpleName());
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) databaseDocumentTx.getStorage();

    paginatedCluster = new OPaginatedClusterV0("paginatedClusterTest", storage);
    paginatedCluster.configure(storage, 42, "paginatedClusterTest", buildDirectory, -1);
    paginatedCluster.create(-1);
  }

}
