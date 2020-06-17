package com.orientechnologies.orient.server;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.config.OServerHookConfiguration;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HookInstallServerTest {
  private static final String SERVER_DIRECTORY = "./target/dbfactory";

  public static class MyHook extends ODocumentHookAbstract {

    public MyHook() {}

    @Override
    public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
      return DISTRIBUTED_EXECUTION_MODE.TARGET_NODE;
    }

    @Override
    public void onRecordAfterCreate(ODocument iDocument) {
      count++;
    }
  }

  private static int count = 0;
  private OServer server;

  @Before
  public void before()
      throws MalformedObjectNameException, InstanceAlreadyExistsException,
          MBeanRegistrationException, NotCompliantMBeanException, ClassNotFoundException,
          NullPointerException, IOException, IllegalArgumentException, SecurityException,
          InvocationTargetException, NoSuchMethodException, InstantiationException,
          IllegalAccessException {
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);

    OServerConfigurationManager ret =
        new OServerConfigurationManager(
            this.getClass()
                .getClassLoader()
                .getResourceAsStream(
                    "com/orientechnologies/orient/server/network/orientdb-server-config.xml"));
    OServerHookConfiguration hc = new OServerHookConfiguration();
    hc.clazz = MyHook.class.getName();
    ret.getConfiguration().hooks = new ArrayList<OServerHookConfiguration>();
    ret.getConfiguration().hooks.add(hc);
    server.startup(ret.getConfiguration());
    server.activate();

    OServerAdmin admin = new OServerAdmin("remote:localhost");
    admin.connect("root", "root");
    admin.createDatabase("test", "nothign", "memory");
    admin.close();
  }

  @After
  public void after() throws IOException {
    OServerAdmin admin = new OServerAdmin("remote:localhost");
    admin.connect("root", "root");
    admin.dropDatabase("test", "memory");
    admin.close();
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    Orient.instance().startup();
  }

  @Test
  public void test() {
    final int initValue = count;

    OPartitionedDatabasePool pool =
        new OPartitionedDatabasePool("remote:localhost/test", "admin", "admin");
    for (int i = 0; i < 10; i++) {
      ODatabaseDocument some = pool.acquire();
      try {
        some.save(new ODocument("Test").field("entry", i));
      } finally {
        some.close();
      }
    }
    pool.close();
    Assert.assertEquals(initValue + 10, count);
  }
}
