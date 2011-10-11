package com.orientechnologies.orient.core.type.tree;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.common.collection.OMVRBTreeCompositeTest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.serialization.serializer.stream.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerLiteral;
import com.orientechnologies.orient.core.storage.OStorage;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class OMVRBTreeDatabaseLazySaveCompositeTest extends OMVRBTreeCompositeTest {
    private ODatabaseDocumentTx database;
    private int oldPageSize;
    private int oldEntryPoints;

    @BeforeClass
    public void beforeClass() {
        oldPageSize = OGlobalConfiguration.MVRBTREE_NODE_PAGE_SIZE.getValueAsInteger();
        OGlobalConfiguration.MVRBTREE_NODE_PAGE_SIZE.setValue(4);

        oldEntryPoints = OGlobalConfiguration.MVRBTREE_ENTRYPOINTS.getValueAsInteger();
        OGlobalConfiguration.MVRBTREE_ENTRYPOINTS.setValue(1);

        database = new ODatabaseDocumentTx("memory:mvrbtreeindextest").create();
        database.addCluster("indextestclsuter", OStorage.CLUSTER_TYPE.MEMORY);
    }


    @AfterClass
    public void afterClass() {
        database.delete();
        OGlobalConfiguration.MVRBTREE_NODE_PAGE_SIZE.setValue(oldPageSize);
        OGlobalConfiguration.MVRBTREE_ENTRYPOINTS.setValue(oldEntryPoints);
    }

    @BeforeMethod
    @Override
    public void beforeMethod() throws Exception {
        tree = new OMVRBTreeDatabaseLazySave<OCompositeKey, Double>(database, "indextestclsuter", OCompositeKeySerializer.INSTANCE,
                OStreamSerializerLiteral.INSTANCE);

        for(double i = 1; i < 4; i++) {
            for(double j = 1; j < 10; j++) {
                final OCompositeKey compositeKey = new OCompositeKey();
                compositeKey.addKey(i);
                compositeKey.addKey(j);
                tree.put(compositeKey, i*4 + j);
            }
        }
        ((OMVRBTreeDatabaseLazySave)tree).save();
        ((OMVRBTreeDatabaseLazySave)tree).optimize(true);
    }
}
