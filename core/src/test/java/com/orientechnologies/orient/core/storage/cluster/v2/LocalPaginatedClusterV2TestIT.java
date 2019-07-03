package com.orientechnologies.orient.core.storage.cluster.v2;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.storage.cluster.LocalPaginatedClusterAbstract;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;

public class LocalPaginatedClusterV2TestIT extends LocalPaginatedClusterAbstract {
  @BeforeClass
  public static void beforeClass() throws IOException {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null || buildDirectory.isEmpty())
      buildDirectory = ".";

    buildDirectory += "/localPaginatedClusterTestV2";

    databaseDocumentTx = new ODatabaseDocumentTx(
        "plocal:" + buildDirectory + File.separator + LocalPaginatedClusterV2TestIT.class.getSimpleName());
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) databaseDocumentTx.getStorage();

    paginatedCluster = new OPaginatedClusterV2("paginatedClusterTest", storage);
    paginatedCluster.configure(42, "paginatedClusterTest");
    paginatedCluster.create();
  }
}
