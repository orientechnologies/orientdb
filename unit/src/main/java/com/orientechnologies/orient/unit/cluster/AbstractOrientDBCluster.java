package com.orientechnologies.orient.unit.cluster;

import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractOrientDBCluster {
    public enum Status {
        OFFLINE, STARTING, ONLINE, SHUTDOWNING
    }

    public enum NodeParams{
        NODE_HOME,
        NODE_NAME,
        NODE_PORT,
        NODE_USER,
        NODE_PWD
    }

    public interface NodeOperationListener {
        void onBefore(int nodeNo, OServer node) throws Exception;
        void onAfter(int nodeNo, OServer node) throws Exception;
    }

    protected static Logger logger = LoggerFactory.getLogger(AbstractOrientDBCluster.class);

    private String name;

    private final AtomicReference<Status> status = new AtomicReference<Status>(Status.OFFLINE);
    protected List<OServer> nodes;

    public AbstractOrientDBCluster(String clusterName, List<OServer> nodes) {
        this.name = clusterName;
        this.nodes = new ArrayList<OServer>(nodes);
    }

    protected Status setStatus(Status status) {
        return this.status.getAndSet(status);
    }

    public OServer getNode(int idx) {
        return nodes.get(idx);
    }

    public final void start() throws Exception {
        start(null);
    }

    protected void startNode(int idx) throws Exception {
        final OServer node = getNode(idx);
        node.activate();
        waitForState(node, Status.ONLINE);
    }

    protected void stopNode(int idx) throws Exception {
        final OServer node = getNode(idx);
        final String nodeName = getNodeParam(idx, NodeParams.NODE_NAME);
        for (Status status = getNodeStatus(node); !Status.OFFLINE.equals(status); status = getNodeStatus(node)) {
            if (status == null) {
                break;
            }

            switch (status) {
                case STARTING:
                case SHUTDOWNING:
                    Thread.sleep(100);
                    break;

                case ONLINE:
                    node.shutdown();
                    logger.trace("Node '{}': Waiting for the node will become OFFLINE", nodeName);
                    break;

                case OFFLINE:
                    break;

                default:
                    throw new IllegalStateException("Unhandled Status value: " + status);
            }
        }
    }

    public synchronized void start(NodeOperationListener listener) throws Exception {
        OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
        logger.debug("Cluster '{}': Starting cluster...", getName());
        setStatus(Status.STARTING);
        for (int i = 0; i < this.size(); ++i) {
            final String nodeName = getNodeParam(i, NodeParams.NODE_NAME);
            logger.debug("Node '{}': Starting node...", nodeName);

            final OServer node = getNode(i);

            System.setProperty(Orient.ORIENTDB_HOME, OSystemVariableResolver.resolveSystemVariables((String) node.getVariable("ORIENTDB_HOME")));
            System.setProperty(OServerConfiguration.PROPERTY_CONFIG_FILE, OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/config/orientdb-server-config.xml"));

            try {
                node.startup();
            } catch (Exception e) {
                logger.error(String.format("\n\nCan't start node '%s' because of error. Cluster '%s' will be stopped.", nodeName, getName()),
                       e);

                for (int j = i - 1; j >= 0; --j) {
                    stop();
                }
            }

            if (listener != null) {
                listener.onBefore(i, node);
            }

            startNode(i);

            if (listener != null) {
                listener.onAfter(i, node);
            }
            logger.info("Node '{}': Node has been started", nodeName);
        }
        setStatus(Status.ONLINE);
        logger.info("Cluster '{}': Cluster is up and running", getName());

    }

    public final void stop() throws Exception {
        stop(null);
    }

    public synchronized void stop(NodeOperationListener listener) throws Exception {
        logger.debug("Cluster '{}': Stopping cluster...", getName());
        setStatus(Status.SHUTDOWNING);

        for (int i = size() - 1; i > -1; --i) {
            final String nodeName = getNodeParam(i,NodeParams.NODE_NAME);
            logger.debug("Node '{}': Stopping node...", nodeName);
            if (listener != null) {
                listener.onBefore(i, getNode(i));
            }

            stopNode(i);

            if (listener != null) {
                listener.onAfter(i, getNode(i));
            }
            logger.info("Node '{}': Node has been stopped", nodeName);
        }
        setStatus(Status.OFFLINE);
        logger.info("Cluster '{}': Cluster has been stopped", getName());
    }

    public String getName() {
        return name;
    }

    public Status getStatus() {
        return status.get();
    }

    public int size() {
        return nodes.size();
    }

    public String getAdminName() {
        return getAdminName(0);
    }

    public String getAdminName(int idx) {
        return getNodeParam(idx, NodeParams.NODE_USER);
    }

    public String getAdminPassword() {
        return getAdminPassword(0);
    }

    public String getAdminPassword(int idx) {
        return getNodeParam(idx, NodeParams.NODE_PWD);
    }


    public String getConnectionString() {
        return getConnectionString(0);
    }

    public String getConnectionString(int idx) {
        return String.format("127.0.0.1:%s", getNodeParam(idx,NodeParams.NODE_PORT));
    }

    protected static OServer waitForState(OServer node, Status state) throws InterruptedException {
        logger.trace("Node '{}': Waiting for the node will become {}", getNodeParam(node, NodeParams.NODE_NAME), state.name());
        for (; ; ) {
            final Status status = getNodeStatus(node);
            if (status != null && state.equals(status)) {
                break;
            }
            Thread.sleep(50);
        }
        return node;
    }

    protected static Status getNodeStatus(OServer node) {
        final ODistributedServerManager dm = node.getDistributedManager();
        if (dm == null) {
            return null;
        }

        final Enum<?> oStatus = dm.getNodeStatus();
        return Status.valueOf(oStatus.name());
    }

    public <T> T getNodeParam(int idx, NodeParams param){
        return (T)getNode(idx).getVariable(param.name());
    }

    static <T> T getNodeParam(OServer node, NodeParams param){
        return (T)node.getVariable(param.name());
    }

    public void setNodeParam(int idx, NodeParams param, Object value){
        setNodeParam(getNode(idx), param, value);
    }
    static void setNodeParam(OServer node, NodeParams param, Object value){
        node.setVariable(param.name(), value);
    }
}