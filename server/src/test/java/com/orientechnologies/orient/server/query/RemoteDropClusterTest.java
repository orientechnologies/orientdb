package com.orientechnologies.orient.server.query;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.*;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE;
import static org.junit.Assert.*;

/**
 * Created by tglman on 03/01/17.
 */
public class RemoteDropClusterTest {

  private static final String SERVER_DIRECTORY = "./target/cluster";
  private OServer           server;
  private OrientDB          orientDB;
  private ODatabaseDocument session;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS.setValue(1);
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    orientDB = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientDB.create(RemoteDropClusterTest.class.getSimpleName(), ODatabaseType.MEMORY);
    session = orientDB.open(RemoteDropClusterTest.class.getSimpleName(), "admin", "admin");
  }

  @Test
  public void simpleDropCluster() {
    int cl = session.addCluster("one");
    session.dropCluster(cl, false);
  }

  @Test
  public void simpleDropClusterTruncate() {
    int cl = session.addCluster("one");
    session.dropCluster(cl, true);
  }

  @Test
  public void simpleDropClusterName() {
    session.addCluster("one");
    session.dropCluster("one", false    );
  }

  @Test
  public void simpleDropClusterNameTruncate() {
    session.addCluster("one");
    session.dropCluster("one", true);
  }

  @After
  public void after() {
    session.close();
    orientDB.close();
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    Orient.instance().startup();
  }

}
