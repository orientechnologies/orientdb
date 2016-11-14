package org.apache.tinkerpop.gremlin.orientdb;

import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestName;

/**
 * Created by Enrico Risa on 14/11/16.
 */

public abstract class OrientGraphBaseTest {

    @Rule
    public TestName name = new TestName();

    protected OrientGraphFactory factory;

    @Rule
    public ExternalResource resource = new ExternalResource() {

        @Override
        protected void before() throws Throwable {

            String config = System.getProperty("orientdb.test.env", "memory");

            String url = null;
            if ("ci".equals(config) || "release".equals(config)) {
                url = "plocal:./target/databases/" + name.getMethodName();
            } else {
                url = "memory:" + name.getMethodName();
            }
            factory = new OrientGraphFactory(url);

        }

        @Override
        protected void after() {
            factory.getNoTx().drop();
        }

    };

}
