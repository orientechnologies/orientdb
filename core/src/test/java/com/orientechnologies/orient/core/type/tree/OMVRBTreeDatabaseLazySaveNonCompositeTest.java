package com.orientechnologies.orient.core.type.tree;

import com.orientechnologies.common.collection.OMVRBTreeNonCompositeTest;
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
public class OMVRBTreeDatabaseLazySaveNonCompositeTest extends OMVRBTreeNonCompositeTest {
    private ODatabaseDocumentTx database;
    private int oldPageSize;
    private int oldEntryPoints;


    @BeforeClass
    public void beforeClass() {
        database = new ODatabaseDocumentTx("memory:mvrbtreeindextest").create();
        database.addCluster("indextestclsuter", OStorage.CLUSTER_TYPE.MEMORY);
    }

    @BeforeMethod
    @Override
    public void beforeMethod() throws Exception {
        oldPageSize = OGlobalConfiguration.MVRBTREE_NODE_PAGE_SIZE.getValueAsInteger();
        OGlobalConfiguration.MVRBTREE_NODE_PAGE_SIZE.setValue(4);

        oldEntryPoints = OGlobalConfiguration.MVRBTREE_ENTRYPOINTS.getValueAsInteger();
        OGlobalConfiguration.MVRBTREE_ENTRYPOINTS.setValue(1);

        tree = new OMVRBTreeDatabaseLazySave<Double, Double>(database, "indextestclsuter", OStreamSerializerLiteral.INSTANCE,
                OStreamSerializerLiteral.INSTANCE);

        for (double i = 1; i < 10; i++) {
            tree.put(i, i);
        }

        ((OMVRBTreeDatabaseLazySave) tree).save();
        ((OMVRBTreeDatabaseLazySave) tree).optimize(true);
    }

    @AfterClass
    public void afterClass() {
        database.delete();

        OGlobalConfiguration.MVRBTREE_NODE_PAGE_SIZE.setValue(oldPageSize);
        OGlobalConfiguration.MVRBTREE_ENTRYPOINTS.setValue(oldEntryPoints);
    }

}
