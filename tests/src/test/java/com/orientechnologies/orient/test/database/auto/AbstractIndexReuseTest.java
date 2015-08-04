package com.orientechnologies.orient.test.database.auto;

import javax.management.remote.JMXConnector;

import com.orientechnologies.common.profiler.OProfiler;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteThread;
import com.orientechnologies.orient.core.Orient;

public abstract class AbstractIndexReuseTest extends DocumentDBBaseTest {
  private JMXConnector     jmxConnector;
  protected OProfiler  profiler;

  public AbstractIndexReuseTest(final String iURL) {
    super(iURL);
  }

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    profiler = getProfilerInstance();
    database.close();

    if (!profiler.isRecording()) {
      profiler.startRecording();
    }
  }

  @AfterClass
  public void closeJMXConnector() throws Exception {
//    if (isRemoteStorage()) {
//      jmxConnector.close();
//    }
  }

  private boolean isRemoteStorage() {
    return database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread;
  }

  private OProfiler getProfilerInstance() throws Exception {
//    if (isRemoteStorage()) {
//      final JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://:10005/jmxrmi");
//      jmxConnector = JMXConnectorFactory.connect(url, null);
//      final MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();
//      final ObjectName onProfiler = new ObjectName("OrientDB:type=Profiler");
    // return JMX.newMBeanProxy(mbsc, onProfiler, OProfiler.class, false);
//    } else {
      return Orient.instance().getProfiler();
//    }
  }
}
