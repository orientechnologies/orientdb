package com.orientechnologies.orient.core.storage.cluster.v1;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.storage.cluster.LocalPaginatedClusterAbstract;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.io.File;
import java.io.IOException;
import org.junit.BeforeClass;

public class LocalPaginatedClusterV1TestIT extends LocalPaginatedClusterAbstract {
  @BeforeClass
  public static void beforeClass() throws IOException {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null || buildDirectory.isEmpty()) buildDirectory = ".";

    buildDirectory += File.separator + LocalPaginatedClusterV1TestIT.class.getSimpleName();
    OFileUtils.deleteRecursively(new File(buildDirectory));

    dbName = "clusterTest";

    final OrientDBConfig config =
        OrientDBConfig.builder()
            .addConfig(OGlobalConfiguration.STORAGE_TRACK_PAGE_OPERATIONS_IN_TX, true)
            .build();
    orientDB = new OrientDB("plocal:" + buildDirectory, config);
    orientDB.create(dbName, ODatabaseType.PLOCAL);

    databaseDocumentTx = (ODatabaseDocumentInternal) orientDB.open(dbName, "admin", "admin");

    storage = (OAbstractPaginatedStorage) databaseDocumentTx.getStorage();

    paginatedCluster = new OPaginatedClusterV1("paginatedClusterTest", storage);
    paginatedCluster.configure(42, "paginatedClusterTest");
    storage
        .getAtomicOperationsManager()
        .executeInsideAtomicOperation(
            null, atomicOperation -> paginatedCluster.create(atomicOperation));
  }
}
