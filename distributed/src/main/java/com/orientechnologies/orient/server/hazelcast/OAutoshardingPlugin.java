package com.orientechnologies.orient.server.hazelcast;

import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDProvider;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OServerHandlerAbstract;
import com.orientechnologies.orient.server.hazelcast.sharding.OAutoshardedStorageImpl;
import com.orientechnologies.orient.server.hazelcast.sharding.distributed.ODHTConfiguration;
import com.orientechnologies.orient.server.hazelcast.sharding.hazelcast.ServerInstance;

/**
 * Distributed plugin implementation that supports autosharding
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 * @since 8/27/12
 */
public class OAutoshardingPlugin extends OServerHandlerAbstract implements ODatabaseLifecycleListener {

  private boolean          enabled = true;
  private ServerInstance   serverInstance;
  private DHTConfiguration dhtConfiguration;

  @Override
  public String getName() {
    return "autosharding";
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    oServer.setVariable("OAutoshardingPlugin", this);

    String configFile = "/hazelcast.xml";
    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (!Boolean.parseBoolean(param.value)) {
          enabled = false;
          return;
        }
      } else if (param.name.equalsIgnoreCase("configuration.hazelcast")) {
        configFile = OSystemVariableResolver.resolveSystemVariables(param.value);
      }
    }

    dhtConfiguration = new DHTConfiguration();

    serverInstance = new ServerInstance(configFile);
    serverInstance.setDHTConfiguration(dhtConfiguration);
  }

  @Override
  public void startup() {
    if (!enabled)
      return;

    serverInstance.init();

    super.startup();
    Orient.instance().addDbLifecycleListener(this);
  }

  @Override
  public void onOpen(ODatabase iDatabase) {
    if (iDatabase instanceof ODatabaseComplex<?>) {
      iDatabase.replaceStorage(new OAutoshardedStorageImpl(serverInstance, (OStorageEmbedded) iDatabase.getStorage(),
							dhtConfiguration));
    }
  }

  @Override
  public void onClose(ODatabase iDatabase) {
  }

  public static class DHTConfiguration implements ODHTConfiguration {

    private final HashSet<String> clusters;

    public DHTConfiguration() {
      clusters = new HashSet<String>();

      clusters.add(OMetadata.CLUSTER_INTERNAL_NAME.toLowerCase());
      clusters.add(OMetadata.CLUSTER_INDEX_NAME.toLowerCase());
      clusters.add(ORole.CLASS_NAME.toLowerCase());
      clusters.add(OUser.CLASS_NAME.toLowerCase());
      clusters.add(OMVRBTreeRIDProvider.PERSISTENT_CLASS_NAME.toLowerCase());
      clusters.add(OSecurityShared.RESTRICTED_CLASSNAME.toLowerCase());
      clusters.add(OSecurityShared.IDENTITY_CLASSNAME.toLowerCase());
      clusters.add(OFunction.CLASS_NAME.toLowerCase());
    }

    @Override
    public Set<String> getDistributedStorageNames() {
      return OServerMain.server().getAvailableStorageNames().keySet();
    }

    @Override
    public Set<String> getUndistributableClusters() {
      return clusters;
    }
  }

}
