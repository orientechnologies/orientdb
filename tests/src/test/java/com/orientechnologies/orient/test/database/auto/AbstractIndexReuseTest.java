package com.orientechnologies.orient.test.database.auto;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteThread;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

public abstract class AbstractIndexReuseTest {
  protected final ODatabaseDocumentTx database;
  private JMXConnector                jmxConnector;
  protected OProfilerMBean            profiler;

  public AbstractIndexReuseTest(final String iURL) {
    database = new ODatabaseDocumentTx(iURL);
  }

  @BeforeClass
  public void setUp() throws Exception {
    database.open("admin", "admin");

    profiler = getProfilerInstance();
    database.close();

    if (!profiler.isRecording()) {
      profiler.startRecording();
    }
  }

  @BeforeMethod
  public void beforeMethod() {
    if (database.isClosed()) {
      database.open("admin", "admin");
    }
  }

  @AfterMethod
  public void afterMethod() {
    database.close();
  }

  @AfterClass
  public void closeJMXConnector() throws Exception {
    if (isRemoteStorage()) {
      jmxConnector.close();
    }
  }

  private boolean isRemoteStorage() {
    return database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread;
  }

  private OProfilerMBean getProfilerInstance() throws Exception {
    if (isRemoteStorage()) {
      final JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://:10005/jmxrmi");
      jmxConnector = JMXConnectorFactory.connect(url, null);
      final MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();
      final ObjectName onProfiler = new ObjectName("OrientDB:type=Profiler");
      return JMX.newMBeanProxy(mbsc, onProfiler, OProfilerMBean.class, false);
    } else {
      return Orient.instance().getProfiler();
    }
  }
}
