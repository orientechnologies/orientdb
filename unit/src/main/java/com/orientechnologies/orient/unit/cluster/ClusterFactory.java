package com.orientechnologies.orient.unit.cluster;

import com.hazelcast.config.*;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.*;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import com.orientechnologies.orient.unit.ConnectionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Properties;

public class ClusterFactory {
    public static class ClusterBuildInfo extends ConnectionInfo {
        private static final int DEFAULT_NUMBER_OF_NODES = 2;

        protected OServerConfiguration serverConfig;
        protected int nodesCount = DEFAULT_NUMBER_OF_NODES;
        protected File clusterBaseDir;
        protected String name = "orient";
        protected int hazelcastPorts[] = {3434};

        public boolean isEmbedded(){
            return nodesCount < 0;
        }

        public boolean isServerConfigProvided(){
            return serverConfig != null;
        }

        private int[] adjustPortsIfNeeded(int ports[]){
            if(nodesCount > ports.length){
                final int retVal[] = Arrays.copyOf(ports, nodesCount);
                int lastPort = ports[ports.length-1];
                for(int i=ports.length; i< nodesCount; ++i ){
                    retVal[i] = ++lastPort;
                }

                return retVal;
            }
            return ports;
        }

        @Override
        public void verify() throws IllegalStateException{
            super.verify();

            throwIllegalStateIf(serverConfig == null, "Server configuration can't be null");
            throwIllegalStateIf(clusterBaseDir == null, "Base cluster directory must be specified");
            throwIllegalStateIf(!isEmbedded() && (hazelcastPorts == null || hazelcastPorts.length < 1),
                    "At least one hazelcast port must be specified");
        }
    }

    public static class ClusterBuildInfoBuilder< T extends ClusterBuildInfoBuilder, I extends ClusterBuildInfo> {
        private final static String HAZELCAST_PORTS_PROPERTY_NAME = "test.hazelcast.ports";
        protected final I buildInfo;

        public ClusterBuildInfoBuilder(I buildInfo){
            this.buildInfo = buildInfo;
        }

        public T clusterName(String name){
            buildInfo.name = name;
            return (T)this;
        }

        /**
         * Setup base cluster dir from the specified property file and key.
         *
         * @param resourcePath path to the properties file
         * @param key key for cluster base dir property
         */
        public T clusterBaseDirFromPropertyFile(String resourcePath, String key) {
            final String rpath = resourcePath.startsWith(  File.separator) ?  resourcePath : File.separator + resourcePath;
            final InputStream is = ClusterFactory.class.getResourceAsStream(rpath);
            try {
                if (is == null) {
                    throw new IllegalStateException(String.format("Can't load properties file, resource '%s' is not found",
                            rpath)
                    );
                }

                final Properties props = new Properties();
                props.load(is);

                clusterBaseDir(props.getProperty(key));

            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    is.close();
                }catch(IOException e){
                }
            }
            return (T)this;
        }


        /**
         * Load server config from the specified resource file
         */
        public T useServerConfigResource(String serverConfigResource){
            final String resourcePath = serverConfigResource.startsWith(  File.separator) ?  serverConfigResource : File.separator + serverConfigResource;

            final InputStream is = ClusterFactory.class.getResourceAsStream(resourcePath);
            try{
                if(is == null) {
                    throw new IllegalStateException(String.format("Can't load OrientDB server configuration, resource '%s' is not found",
                            resourcePath)
                    );
                }

                final OServerConfigurationLoaderXml loader = new OServerConfigurationLoaderXml(OServerConfiguration.class, is);
                buildInfo.serverConfig = loader.load();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally{
                try {
                    is.close();
                }catch(IOException e){
                }
            }

            // -- setup base cluster directory, if provided
            final String clusterBaseDir = buildInfo.serverConfig.getProperty(Orient.ORIENTDB_HOME);
            if(clusterBaseDir != null && !clusterBaseDir.isEmpty()){
                buildInfo.clusterBaseDir = new File(clusterBaseDir);
            }

            // -- setup hazelcast ports, if provided
            final String rawHazelcastPorts = buildInfo.serverConfig.getProperty(HAZELCAST_PORTS_PROPERTY_NAME);
            if(rawHazelcastPorts != null && !rawHazelcastPorts.isEmpty()){

            }

            return (T)this;
        }

        public T numberOfNodes(int count){
            buildInfo.nodesCount = count;
            return (T)this;
        }

        public T clusterBaseDir(String pathToBaseClusterDir){
            buildInfo.clusterBaseDir = new File(pathToBaseClusterDir);
            return (T)this;
        }

        /**
         * Use specified hazelcast ports, one per node.
         *
         * Hazelcast ports also might be provided in server configuration file, as a value of 'test.hazelcast.ports' variable.
         */
        public T useHazelcastPorts(int...ports){
            buildInfo.hazelcastPorts = ports;
            return (T)this;
        }

        /**
         * Use specified ports for binary protocol, one per node.
         *
         * This val ports also might be provided in server configuration file, as a value of 'test.hazelcast.ports' variable.
         */
        public T useBinaryPorts(int...ports){
            buildInfo.setBinaryPorts(ports);
            return (T)this;
        }

        /**
         * Run "Embedded" OrientDb instance (Non distributed single node configuration)
         */
        public T useEmbeddedServer(){
            buildInfo.nodesCount = -1;
            return (T)this;
        }
    }

    private final static String ORIENT_DB_PATH_PROPERTY_NAME = "server.database.path";
    private static Logger logger = LoggerFactory.getLogger(ClusterFactory.class);

    public static AbstractOrientDBCluster createCluster(ClusterBuildInfo buildInfo) throws Exception {

        final boolean buildEmbedded = buildInfo.isEmbedded();

        // -- verify and adjust
        buildInfo.verify();

        /**
         * At the moment, due to the OrientDb issues there is no chance to get embedded OrientDb cluster up and running on Windows
         */
        if( !buildEmbedded && (buildInfo.nodesCount > 1) ){
            final String osName = System.getProperty("os.name").toLowerCase();
            if(osName.startsWith("windows")){
                logger.warn("\n\n\n\tNumber of nodes in the Embedded OrientDB cluster was forced to use only a Single Node because of problem with running cluster of OrientDB on Windows\n\n\n");
                buildInfo.nodesCount = 1;
            }
        }

        if( !buildEmbedded) {
            buildInfo.hazelcastPorts = buildInfo.adjustPortsIfNeeded(buildInfo.hazelcastPorts);
        }

        buildInfo.setBinaryPorts( buildInfo.adjustPortsIfNeeded(buildInfo.getBinaryPorts()));


        // --
        final int nmbOfNode = buildEmbedded ? 1 : buildInfo.nodesCount;
        final ArrayList<OServer> nodes = new ArrayList<OServer>(nmbOfNode);

        final Config hazelcastConfig = buildEmbedded ? null : buildTCPHazelcastConfig(buildInfo.name, buildInfo.getAddress(), buildInfo.hazelcastPorts);
        for(int i=0; i<nmbOfNode; ++i){
            final String nodeName = buildEmbedded ? "embedded" : "node-" + i;
            final OServer node = createNode(buildInfo.serverConfig, buildInfo.clusterBaseDir, nodeName, buildInfo.getAddress(),
                    buildInfo.getLogin(), buildInfo.getPassword(), buildInfo.getBinaryPorts()[i],
                    buildEmbedded ? -1 : buildInfo.hazelcastPorts[i], hazelcastConfig);
            nodes.add(node);
        }

        if(buildEmbedded){
            return new AbstractOrientDBCluster("Embedded", nodes) {
                @Override
                protected void startNode(int idx) throws Exception {
                    final OServer node = getNode(idx);
                    node.activate();
                }

                @Override
                protected void stopNode(int idx) throws Exception {
                    final OServer node = getNode(idx);
                    node.shutdown();
                }
            };
        } else {
            return new AbstractOrientDBCluster(hazelcastConfig.getGroupConfig().getName(), nodes) {
            };
        }
    }

    public static AbstractOrientDBCluster rebuildCluster(AbstractOrientDBCluster cluster) {
        return new MixedOrientDBCluster(cluster);
    }

    private static OServer createNode(OServerConfiguration generalConfig, File orientHome, String nodeName,
                   String address, String userName, String password, int binaryPort, int hazelcastPort, Config hazelcastConfig) throws Exception {
        final File nodeHome = new File(orientHome, nodeName);

        // -- hazelcast setup, if provided (may be null in case of creating embedded server)
        if(hazelcastConfig != null) {
            hazelcastConfig.getNetworkConfig().setPort(hazelcastPort);
        }

        // -- binary protocol setup
        final OServerNetworkListenerConfiguration binaryListener = getOrCreateNetListenerByClass(generalConfig, ONetworkProtocolBinary.class);
        binaryListener.portRange = Integer.toString(binaryPort);
        if(address.equals("localhost") || address.equals("127.0.0.1")){
            binaryListener.ipAddress = "0.0.0.0";
        } else {
            binaryListener.ipAddress = address;
        }

        // --  user
        if(generalConfig.users == null){
            generalConfig.users = new OServerUserConfiguration[0];
        }

        boolean userAlreadyConfigured = false;
        for(int i=0; i< generalConfig.users.length; ++i){
            final OServerUserConfiguration ucfg = generalConfig.users[i];

            if(!userName.equalsIgnoreCase(ucfg.name)){
                continue;
            }

            if(!"*".equals(ucfg.resources)){
                ucfg.resources = "*";
            }

            if(!password.equals(ucfg.password)){
                ucfg.password = password;
            }

            userAlreadyConfigured = true;
            break;
        }

        if(!userAlreadyConfigured){
            final OServerUserConfiguration ucfg = new OServerUserConfiguration();
            ucfg.name = userName;
            ucfg.password = password;
            ucfg.resources = "*";
            generalConfig.users = Arrays.copyOf(generalConfig.users, generalConfig.users.length +1);
            generalConfig.users[generalConfig.users.length -1] = ucfg;
        }

        // --
        final OServerConfiguration config =  prepareNodeConfig(generalConfig, nodeHome, hazelcastConfig);

        String adminName = null, adminPassword = null;
        for(OServerUserConfiguration ucfg: config.users){
            if("*".equals(ucfg.resources)){
                adminName = ucfg.name;
                adminPassword = ucfg.password;
                break;
            }
        }

        final OServer node = OServerMain.create();
        node.setVariable(Orient.ORIENTDB_HOME, nodeHome.getAbsolutePath());
        AbstractOrientDBCluster.setNodeParam(node, AbstractOrientDBCluster.NodeParams.NODE_HOME, nodeHome);
        AbstractOrientDBCluster.setNodeParam(node, AbstractOrientDBCluster.NodeParams.NODE_NAME, nodeName);
        AbstractOrientDBCluster.setNodeParam(node, AbstractOrientDBCluster.NodeParams.NODE_PORT, binaryPort);
        AbstractOrientDBCluster.setNodeParam(node, AbstractOrientDBCluster.NodeParams.NODE_USER, adminName);
        AbstractOrientDBCluster.setNodeParam(node, AbstractOrientDBCluster.NodeParams.NODE_PWD, adminPassword);

        node.setServerRootDirectory( nodeHome.getAbsolutePath());
        return node;
    }

    private static OServerConfiguration prepareNodeConfig(OServerConfiguration generalConfiguration, File nodeHome,
            com.hazelcast.config.Config hazelcastConfig) throws Exception {

        final String nodeName = nodeHome.getName();

        //  ----------  setup node db path
        for(int i=0; i<generalConfiguration.properties.length; ++i){
            final OServerEntryConfiguration prop = generalConfiguration.properties[i];
            if(prop.name.equals(ORIENT_DB_PATH_PROPERTY_NAME)){
                prop.value = new File(nodeHome, "databases").getAbsolutePath();
            } else if(prop.name.equals(Orient.ORIENTDB_HOME)){
                prop.value = nodeHome.getAbsolutePath();
            }
        }

        //  ----------  create new instance of Server configuration
        final StringWriter sw = new StringWriter();
        Utils.writeToXml(sw, generalConfiguration);

        final OServerConfiguration retVal;
        final InputStream is = new ByteArrayInputStream(sw.toString().getBytes());
        try {
            final OServerConfigurationLoaderXml loader = new OServerConfigurationLoaderXml(OServerConfiguration.class, is);
            retVal = loader.load();
        }finally{
            is.close();
        }

        // ----------  create or re-create directories for node
        final File configDir = new File(nodeHome, "config");
        OFileUtils.deleteRecursively(nodeHome);

        if(!configDir.mkdirs()){
            throw new IllegalStateException(String.format("Can't create node config directory '%s'",
                    configDir.getAbsolutePath()
            ));
        }

        final boolean isDistributedNode = hazelcastConfig != null;
        if( isDistributedNode ) {
            // ---------- create hazelcast handler
            final String HAZELCAST_PLUGIN_CONFIG = "<handler class=\"com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin\">\n" +
                    "            <parameters>\n" +
                    "                <parameter value=\"true\" name=\"enabled\"/>\n" +
                    "                <parameter value=\"%1$s/config/default-distributed-db-config.json\" name=\"configuration.db.default\"/>\n" +
                    "                <parameter value=\"%1$s/config/hazelcast.xml\" name=\"configuration.hazelcast\"/>\n" +
                    "                <parameter value=\"com.orientechnologies.orient.server.distributed.conflict.ODefaultReplicationConflictResolver\" name=\"conflict.resolver.impl\"/>\n" +
                    "                <parameter value=\"com.orientechnologies.orient.server.hazelcast.sharding.strategy.ORoundRobinPartitioninStrategy\" name=\"sharding.strategy.round-robin\"/>\n" +
                    "                <parameter value=\"%2$s\" name=\"nodeName\"/>\n" +
                    "            </parameters>\n" +
                    "        </handler>";

            final OServerHandlerConfiguration hazelcastHandler = Utils.readFromXML(OServerHandlerConfiguration.class, HAZELCAST_PLUGIN_CONFIG, nodeHome, nodeName);

            if (retVal.handlers == null) {
                retVal.handlers = new LinkedList();
            }
            retVal.handlers.add(hazelcastHandler);

            // -- distributed DB
            final String DEFAULT_DISTRIBUTED_DB_CONFIG_v17 = "{\n" +
                    "    \"autoDeploy\": true,\n" +
                    "    \"hotAlignment\": false,\n" +
                    "    \"readQuorum\": 1,\n" +
                    "    \"writeQuorum\": 2,\n" +
                    "    \"failureAvailableNodesLessQuorum\": false,\n" +
                    "    \"readYourWrites\": true,\n" +
                    "    \"clusters\": {\n" +
                    "        \"internal\": {\n" +
                    "        },\n" +
                    "        \"index\": {\n" +
                    "        },\n" +
                    "        \"*\": {\n" +
                    "            \"servers\" : [ \"<NEW_NODE>\" ]\n" +
                    "        }\n" +
                    "    }\n" +
                    "}";
            Utils.writeAsFile(new File(configDir, "default-distributed-db-config.json"), DEFAULT_DISTRIBUTED_DB_CONFIG_v17);


            // -- Hazelcast
            final ConfigXmlGenerator hazelcastConfigXmlGenerator = new ConfigXmlGenerator(true);
            final String cfg = hazelcastConfigXmlGenerator.generate(hazelcastConfig);

            Utils.writeAsFile(new File(configDir, "hazelcast.xml"), cfg);
        }

        // ---------- create graph handler (for gremlin)
//        String GRAPH_HANDLER_CONFIG = "<handler class=\"com.orientechnologies.orient.graph.handler.OGraphServerHandler\">\n" +
//                "            <parameters>\n" +
//                "                <parameter value=\"true\" name=\"enabled\"/>\n" +
//                "                <parameter value=\"50\" name=\"graph.pool.max\"/>\n" +
//                "            </parameters>\n" +
//                "        </handler>\n";
//        final OServerHandlerConfiguration graphHandler = readFromXML(OServerHandlerConfiguration.class, GRAPH_HANDLER_CONFIG);
//        retVal.handlers.add(graphHandler);

        Utils.writeAsXmlFile(new File(nodeHome, OServerConfiguration.DEFAULT_CONFIG_FILE), retVal);
        return retVal;
    }

    private static OServerNetworkListenerConfiguration getOrCreateNetListenerByClass(OServerConfiguration config, Class clazz){
        if(config.network == null) {
            config.network = new OServerNetworkConfiguration();
            config.network.protocols = new LinkedList<OServerNetworkProtocolConfiguration>();
        }

        String listenerName = null;
        for (OServerNetworkProtocolConfiguration proto : config.network.protocols) {
            if (clazz.getName().equals(proto.implementation)) {
                listenerName = proto.name;
                break;
            }
        }

        if (listenerName == null) {
            final OServerNetworkProtocolConfiguration protocolConfiguration = new OServerNetworkProtocolConfiguration();
            listenerName = clazz.getSimpleName();
            protocolConfiguration.name = listenerName;
            protocolConfiguration.implementation = clazz.getName();
            config.network.protocols.add(config.network.protocols.size(), protocolConfiguration);
        }

        if(config.network.listeners == null){
            config.network.listeners = new LinkedList<OServerNetworkListenerConfiguration>();
        }
        OServerNetworkListenerConfiguration listener = null;
        for (OServerNetworkListenerConfiguration l : config.network.listeners) {
            if (listenerName.equals(l.protocol)) {
                listener = l;
                break;
            }
        }

        if (listener == null) {
            listener = new OServerNetworkListenerConfiguration();
            listener.protocol = listenerName;
            config.network.listeners.add(config.network.listeners.size(), listener);
        }
        return listener;
    }

    private static com.hazelcast.config.Config buildTCPHazelcastConfig(String clusterName, String address, int hazelcastPorts[]){
        final com.hazelcast.config.Config cfg = new com.hazelcast.config.Config();

        cfg.setGroupConfig( new GroupConfig(clusterName, clusterName) );

        final NetworkConfig networkConfig = new NetworkConfig();
        networkConfig.setPort(-1);
        networkConfig.setPortAutoIncrement(true);

        final JoinConfig joinConfig = new JoinConfig();

        joinConfig.setMulticastConfig(new MulticastConfig());
        joinConfig.getMulticastConfig().setEnabled(false);

        joinConfig.setTcpIpConfig(new TcpIpConfig());
        joinConfig.getTcpIpConfig().setEnabled(true);

        for(int port: hazelcastPorts){
            joinConfig.getTcpIpConfig().addMember(String.format("%s:%s", address, port));
        }

        networkConfig.setJoin(joinConfig);

        cfg.setNetworkConfig(networkConfig);

        final ExecutorConfig executorConfig = new ExecutorConfig();
        executorConfig.setPoolSize(16);

        cfg.addExecutorConfig(executorConfig);
        return cfg;
    }

}
