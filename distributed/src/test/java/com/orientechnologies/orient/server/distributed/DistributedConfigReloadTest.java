package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.OCommandSQL;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public final class DistributedConfigReloadTest {

    private String dbName = "distconfigreloaddb";

    public DistributedConfigReloadTest(String dbName) {
        this.dbName = dbName;
        createDBData();
    }

    public void createDBData() {
        if (checkAndCreateDatabase(dbName)) {
            log("Creating data for database " + dbName);
            int totalClassCount = 50;
            ODatabaseDocumentTx orientGraph = new ODatabaseDocumentTx(getDBURL());
            if(orientGraph.exists()){
                orientGraph.open("admin", "admin");
            }else{
                orientGraph.create();
            }
            createVertexType(orientGraph, "Test", "property1", "property2");
            for (int i = 1; i <= totalClassCount; i++) {
                createVertexType(orientGraph, "Test"+i, "property1", "property2");
            }
            orientGraph.close();

            ODatabaseDocument graph = getGraphFactory().acquire();
            graph.begin();
            for (int i = 1; i <= 100; i++) {
                OVertex vertex = graph.newVertex("TestNode");
                vertex.setProperty("property1", "value1-" + i);
                vertex.setProperty("property2", "value2-" + i);
                vertex.setProperty("property3", "value3-" + i);
                vertex.setProperty("property4", "value4-" + i);
                vertex.setProperty("prop-6", "value6-"+ i);
                vertex.setProperty("prop-7", "v7-1");
                vertex.setProperty("prop-8", "v7-1");
                vertex.setProperty("prop-9", "v7-1");
                vertex.setProperty("prop-10", "v7-1");
                vertex.setProperty("prop11", "v7-1");
                vertex.setProperty("prop12", "v7-1");
                vertex.setProperty("prop13", "v7-1");
                vertex.setProperty("prop14", "v7-1");
                vertex.setProperty("prop15", System.currentTimeMillis());
                graph.commit();
                if ((i % 100) == 0) {
                    log("Created " + i + " nodes");
                }
            }
            graph.close();
            log("Done. Creating data for database " + dbName);
        }
    }

    public void createVertexType(ODatabaseDocument orientGraph, String className, String property, String keyIndexProperty) {
        String edgeClassName = className;
        String vertexClassName = className+"Node";
        OClass clazz = orientGraph.getClass(edgeClassName);
        if (clazz == null) {
            log("Creating edge type - " + edgeClassName);
            clazz = orientGraph.createEdgeClass(edgeClassName);
            clazz.createProperty("linkKey", OType.STRING);
            orientGraph.getClass(edgeClassName).createIndex(edgeClassName+".linkKey", INDEX_TYPE.UNIQUE, "linkKey");
        } else {
            log("Edge " +edgeClassName + " already exists");
        }
        clazz = orientGraph.getClass(vertexClassName);
        if (clazz == null) {
            log("Creating vertex type - " + vertexClassName);
            clazz = orientGraph.createVertexClass(vertexClassName);
            clazz.createProperty(property, OType.STRING);
            clazz.createProperty(keyIndexProperty, OType.STRING);
            orientGraph.getClass(vertexClassName).createIndex(vertexClassName+"."+keyIndexProperty, INDEX_TYPE.UNIQUE, keyIndexProperty);
            clazz.createIndex(vertexClassName + "_Index_" + property, INDEX_TYPE.UNIQUE, property);
        } else {
            log("Class " + vertexClassName + " already exists");
        }
    }

    public void runTest() {
        ExecutorService executorService = Executors.newFixedThreadPool(50);
        List<Future<?>> ths = new ArrayList();

        for (int i = 1 ; i < 30; i ++) {
            String edgeName = "Test"+i;
            String className = edgeName + "Node";
            Future<?> future = executorService.submit(startCreateDeleteVertex(i, getGraphFactory(), className));
            ths.add(future);
        }
        for (Future<?> th : ths) {
            try {
                th.get();
            } catch (Exception ex) {
                System.out.println("********** Future Exception " + ex);
                ex.printStackTrace();
            }
        }
    }

    private Runnable startCreateDeleteVertex(final int id, final ODatabasePool graphFactory, final String className) {
        Runnable th = new Runnable() {
            @Override
            public void run() {
                log("Starting runnable to create index " + className);
                long st = System.currentTimeMillis();
                ODatabaseDocument graph = graphFactory.acquire();
                try {
                    boolean isRunning = true;
                    for (int j=0; j < 1000000; j++) {
                        String sql = "DELETE VERTEX " + className;
                        try {
                            graph.command(new OCommandSQL(sql)).execute();
                            graph.commit();
                            log(" [" + id + "] Delete vertex : [" +j + "] [" + className + "]");
                        } catch (Exception ex) {
                            log("[" + j + "] Failed to delete vertex  [" + className + "][" + ex + "]");
                        }
                        for (int i = 1; i < 2000 && isRunning; i++) {
                            if ((i % 1000) == 0) {
                                long et = System.currentTimeMillis();
                                log(" [" + id + "] Total Records Processed: [" + i + "] Time taken for [100] records: [" + (et-st)/1000 + "] seconds");
                                st = System.currentTimeMillis();
                            }

                            OVertex vertex = graph.newVertex(className);
                            try {
                                vertex.setProperty("property1", "value1-" + i);
                                vertex.setProperty("property2", "value2-" + (System.currentTimeMillis() + "-" + i));
                                vertex.setProperty("property3", "value3-" + i);
                                vertex.setProperty("property4", "value4-1");
                                vertex.setProperty("prop-6", "value6-"+ i);
                                vertex.setProperty("prop-7", "value7-"+ i);
                                vertex.setProperty("prop-8", "value7-1");
                                vertex.setProperty("prop-9", "value7-1");
                                vertex.setProperty("prop-10", "value7-1");
                                vertex.setProperty("prop11", "value7-1");
                                vertex.setProperty("prop12", "value7-1");
                                vertex.setProperty("prop13", "value7-1");
                                vertex.setProperty("prop14", System.currentTimeMillis());
                                vertex.setProperty("prop15", System.currentTimeMillis());
                                graph.commit();
                            } catch (ONeedRetryException  ex) {
                                if (ex instanceof ONeedRetryException || ex.getCause() instanceof ONeedRetryException) {
                                    log("[" + id + "] OrientDB Retry Exception [" + ex + "]");
                                } else {
                                    log("[" + id + "] OrientDB Exception [" + ex + "]");
                                }
                                if (!(ex instanceof ODistributedConfigurationChangedException || ex.getCause() instanceof ODistributedConfigurationChangedException)) {
                                    //reloadVertex(vertex, ex);
                                } else {
                                    log("["+ id +"] ODistributedConfigurationChangedException {} while updating vertex " + vertex);
                                }
                            } catch (ODistributedException ex) {
                                if (ex.getCause() instanceof ONeedRetryException) {
                                    log("[" + id + "] OrientDB Retry Exception [" + ex + "]");
                                } else {
                                    log("[" + id + "] OrientDB Exception [" + ex + "]");
                                }
                                if (!(ex.getCause() instanceof ODistributedConfigurationChangedException)) {
                                    //reloadVertex(vertex, ex);
                                } else {
                                    log("["+ id +"] ODistributedConfigurationChangedException {} while updating vertex " + vertex);
                                }
                            } catch (Exception ex) {
                                log("[" + id + "] Failed non OrientDB Exception [" + ex + "]");
                            }
                        }
                    }
                } finally {
                    graph.close();
                }
            }
        };
        return th;
    }

    public static final void log(String message) {
        System.out.println("[" + Thread.currentThread().getName() +"][" + getDate() + "] " + message);
    }

    public static final void log(String message, Throwable th) {
        System.out.println("[" + Thread.currentThread().getName() +"][" + getDate() + "] " + message);
        th.printStackTrace();
    }

    /**
     * @return
     */
    private static String getDate() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return formatter.format(new Date());
    }

    private ODatabasePool graphReadFactory;

    /**
     * @return
     */
    public String getDBURL() {
        String dataBaseURL = System.getProperty("db.url", "localhost:2424;localhost:2425;localhost:2426");
        return "remote:" + dataBaseURL + "/" + dbName;
    }

    private ODatabasePool getGraphFactory() {
        if (graphReadFactory == null) {
            log("Datastore pool created with size : 50, db location: " + getDBURL());
            graphReadFactory = OrientDB.fromUrl(getDBURL().substring(0, getDBURL().length() - (dbName.length() + 1)).replaceFirst("plocal", "embedded"), OrientDBConfig.defaultConfig()).openPool(dbName, "admin", "admin");
        }
        return graphReadFactory;
    }

    /**
     *
     */
    public boolean checkAndCreateDatabase(String dbName) {
        boolean isNewDB = false;
        try {
            OServerAdmin serverAdmin = new OServerAdmin(getDBURL()).connect("root", "database");
            if (!serverAdmin.existsDatabase("plocal")) {
                log("Database does not exists. New database is created");
                serverAdmin.createDatabase(dbName,"graph", "plocal");
                ODatabaseDocumentTx orientGraph = new ODatabaseDocumentTx(getDBURL());
                if(orientGraph.exists()){
                  orientGraph.open("admin", "admin");
                }else{
                  orientGraph.create();
                }
                orientGraph.command(new OCommandSQL("ALTER DATABASE custom strictSQL=false")).execute();
                orientGraph.close();
                isNewDB = true;
            } else {
                log(dbName + " database already exists");
            }
            serverAdmin.close();
        } catch (Exception ex) {
            log("Failed to create database", ex);
        }
        return isNewDB;
    }

    public static void main(String[] args) {
        DistributedConfigReloadTest test = new DistributedConfigReloadTest("configreloaddb");
        test.runTest();
    }

}
