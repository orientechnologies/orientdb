package com.orientechnologies.orient.unit.cluster;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.OServerShutdownMain;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * All cluster nodes of this cluster runs in their own JVMs. The only first node uses JVM in which this cluster instance was created.
 *
 * Cluster takes care about spawning and destroying JVMs.
 */
class MixedOrientDBCluster extends AbstractOrientDBCluster {
    static {
        logger = LoggerFactory.getLogger(MixedOrientDBCluster.class);
    }

    private Process spawned[];

    public MixedOrientDBCluster(String clusterName, List<OServer> nodes) {
        super(clusterName, nodes);
        spawned = new Process[nodes.size()];
    }

    MixedOrientDBCluster(AbstractOrientDBCluster cluster){
        this(cluster.getName(), cluster.nodes);
    }

    private static int getQntOfClusterMembers(OServer node){
        final ODocument doc = node.getDistributedManager().getClusterConfiguration();
        final List<ODocument> members = doc.field("members");
        return members.size();
    }

    @Override
    protected void stopNode(int idx) throws Exception {
        final OServer node = getNode(idx);
        final String nodeName = getNodeParam(idx, NodeParams.NODE_NAME);

        if(idx == 0){
            super.stopNode(0);
        } else {
            final Process p = spawned[idx];
            if(p != null){
                logger.trace("Node '{}': Spawning shutdown process", nodeName);
                Process pShutdown = spawnNodeProcess(node, OServerShutdownMain.class, true);

                logger.trace("Node '{}': Waiting shutdown process completion", nodeName);
                int i = pShutdown.waitFor();
                pShutdown.destroy();

                logger.trace("Node '{}': Waiting completion of  process", nodeName);
                try {
                    p.wait(750);
                } catch (Exception ex){
                    logger.trace(String.format("Node '%s': Got an exception while waiting process completion", nodeName), ex);
                }
                if(!isProcessFinished(p)){
                    logger.warn("Node '{}': Process termination timeout reached, process will be terminated forcibly", nodeName);
                }

                p.destroy();
                spawned[idx] = null;
            }
        }
    }

    boolean isProcessFinished(Process p){
        try {
            p.exitValue();
            return true;
        }catch(IllegalThreadStateException e){
        }
        return false;
    }

    @Override
    protected void startNode(int idx) throws Exception {
        final OServer node = getNode(idx);
        final String nodeName = getNodeParam(idx, NodeParams.NODE_NAME);

        if(idx == 0){
            node.startup();
            super.startNode(0);
        } else {
            final Process p = spawnNodeProcess(node, OServerMain.class, true);
            spawned[idx] = p;
            final OServer firstNode = getNode(0);

            logger.trace("Node '{}': Waiting for the node will become ONLINE", nodeName);
            for(boolean nodeOnline = false; !nodeOnline;) {
                nodeOnline = getQntOfClusterMembers(firstNode) == idx + 1;
                if(isProcessFinished(p)) {
                    throw new RuntimeException(
                            String.format("Node '%s': node failed to start, spawned process hs been finished unexpectedly",
                                    nodeName)
                        );
                }
                Thread.sleep(100);
            }
        }
    }

    private static Process spawnNodeProcess(OServer node, Class clazz, boolean redirectStream) throws Exception {
        final File nodeHome = (File)node.getVariable(NodeParams.NODE_HOME.name());

        String separator = System.getProperty("file.separator");
        String classpath = System.getProperty("java.class.path");


        String path = System.getProperty("java.home")
                + separator + "bin" + separator + "java";

        final List<String>  args = Arrays.asList(
                path,
                "-cp",
                classpath,

//                "-Xdebug",
//                "-Xnoagent",
//                "-Djava.compiler=NONE",
//                "-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5007",

                "-Dorientdb.config.file=${ORIENTDB_HOME}/" + OServerConfiguration.DEFAULT_CONFIG_FILE,
                "-Djava.util.logging.config.file=${ORIENTDB_HOME}/config/orientdb-server-log.properties",
                clazz.getCanonicalName()
        );

        final ProcessBuilder processBuilder = new ProcessBuilder(args);
        processBuilder.redirectErrorStream(redirectStream);
        processBuilder.directory(nodeHome);

        processBuilder.environment().put(Orient.ORIENTDB_HOME, nodeHome.getAbsolutePath());

        final Process process = processBuilder.start();
        return process;
    }
}