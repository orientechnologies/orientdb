package com.orientechnologies.orient.server.distributed.ridbag;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.stream.Collectors;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import org.junit.Test;

public class RidBagConversionIT {

  @Test
  public void testConversion()
      throws IOException, InstantiationException, InvocationTargetException, NoSuchMethodException,
          MBeanRegistrationException, IllegalAccessException, InstanceAlreadyExistsException,
          NotCompliantMBeanException, ClassNotFoundException, MalformedObjectNameException {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(1);
    OrientDB orientDB = new OrientDB("embedded:target/server0/", OrientDBConfig.defaultConfig());
    orientDB.create("test", ODatabaseType.PLOCAL);
    ODatabaseSession database = orientDB.open("test", "admin", "admin");
    database.begin();
    OVertex ver = database.newVertex("V");
    OVertex ver1 = database.newVertex("V");
    OVertex ver2 = database.newVertex("V");
    OEdge e = database.newEdge(ver, ver1, "E");
    OEdge e1 = database.newEdge(ver, ver2, "E");
    database.save(e);
    database.save(e1);
    database.commit();
    database.close();
    orientDB.close();
    OServer server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    OServer server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");

    OrientDB embOri = server0.getContext();
    ODatabaseSession data = embOri.open("test", "admin", "admin");
    try (OResultSet query = data.query("select from V")) {
      for (OResult res : query.stream().collect(Collectors.toList())) {
        OElement ele = res.getElement().get();
        ele.setProperty("name", "val");
        data.save(ele);
      }
    }
    data.close();
    embOri.drop("test");
    embOri.close();
    server0.shutdown();
    server1.shutdown();
  }
}
