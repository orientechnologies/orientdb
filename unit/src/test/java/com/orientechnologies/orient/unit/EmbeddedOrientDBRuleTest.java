package com.orientechnologies.orient.unit;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseFlat;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertNotNull;

@FixMethodOrder(MethodSorters.JVM)
public class EmbeddedOrientDBRuleTest {

    @ClassRule
    public static ODBTestSupportRule embeddedOrientDB = ODBTestSupportRule.create(
            EmbeddedOrientDBRule.build()
                    .clusterBaseDirFromPropertyFile("test-orientdb.properties", "cluster-base-dir")
                    .createGraphDB()
    );

    private final static Logger logger = LoggerFactory.getLogger(EmbeddedOrientDBRuleTest.class);

    @Test
    public void simpleConnectionCheck(){
        final ODatabaseFlat db = embeddedOrientDB.localDatabaseFlat();
        try {
            final StringBuilder builder = new StringBuilder(
                    String.format("\nClasses available at %s", db.getURL())
                );

            for( OClass cls: db.getMetadata().getSchema().getClasses()){
                builder.append("\n\t")
                    .append(cls.getName());
            }

            logger.info(builder.toString());
        } finally {
            db.close();
        }
    }

//    @Ignore("Ignoring  a strange error somewhere into the ODB internals")
    @Test
    public void checkThatNewlyCreatedClassIsAvailableOnEachClusterNode() throws Exception {

        // -- preparation
        embeddedOrientDB.executeSQLCommandWithParams("create class V1 extends V");
        embeddedOrientDB.executeSQLCommandWithParams("create vertex V1 set prop = '12'");


        // -- verification
        for(int i= embeddedOrientDB.nodesCount() - 1; i<=0; --i){
            final ODatabaseDocumentTx db = embeddedOrientDB.remoteDocumentDBFromGPool(i);
            try{
                final OSchema schema = db.getMetadata().getSchema();
                final OClass oclass = schema.getClass("V2");

                assertNotNull(String.format("Class V1 doesn't exists on node %d", i), oclass);
            }finally {
                db.close();
            }
        }
    }
}
