package org.apache.tinkerpop.gremlin.orientdb;

import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class OPartitionedReCreatableDatabasePoolTest {

    protected OPartitionedReCreatableDatabasePool pool() {
        return new OPartitionedReCreatableDatabasePool("memory:" + Math.random(), "admin", "admin", 5, true);
    }

    @Test
    public void testDatabaseAcquiredByOPartitionedReCreatableDatabasePool() throws Exception {
        OPartitionedReCreatableDatabasePool pool = pool();
        assertFalse(pool.acquire().isClosed());

        pool.close();
        assertNull(pool.acquire());

        pool.reCreatePool();
        assertFalse(pool.acquire().isClosed());
    }
}
