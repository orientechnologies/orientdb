package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Created by olena.kolesnyk on 28/07/2017.
 */
public class CreateMemoryDatabaseFixture {

    protected static ODatabase database;
    protected static OrientDB factory;
    private static final String PATH = "memory";
    private static final String DB_NAME = "test_database";
    private static final String USER = "admin";
    private static final String PASSWORD = "admin";

    @BeforeClass
    public static void setUp() {
        factory = new OrientDB(PATH, OrientDBConfig.defaultConfig());
        factory.create(DB_NAME, ODatabaseType.MEMORY);
        database = factory.open(DB_NAME, USER, PASSWORD);
    }

    @AfterClass
    public static void tearDown() {
        database.close();
        factory.drop(DB_NAME);
        factory.close();
    }

}
