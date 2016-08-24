/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import com.orientechnologies.orient.core.sql.query.OLiveResultListener;
import com.orientechnologies.orient.core.sql.query.OResultSet;
import com.orientechnologies.orient.core.storage.OCluster;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by luigidellaquila on 13/04/15.
 */
@Test
public class OLiveQueryTest {

    private CountDownLatch latch = new CountDownLatch(7);

    class MyLiveQueryListener implements OLiveResultListener {

        public int rowCount = 0;

        @Override
        public void onLiveResult(int iLiveToken, ORecordOperation iOp) throws OException {
            switch (iOp.type) {
                case ORecordOperation.CREATED: {
                    this.rowCount++;
                    break;
                }
                case ORecordOperation.DELETED: {
                    this.rowCount--;
                    break;
                }
            }
            
            System.out.println();
            System.out.println("Operation :" + ORecordOperation.getName(iOp.type));            
            System.out.println("Data: " + iOp.getRecord().toJSON());
            System.out.println();
            System.out.println("----------------------------------------------------------");

            latch.countDown();
        }

        @Override
        public void onError(int iLiveToken) {

        }

        @Override
        public void onUnsubscribe(int iLiveToken) {

        }
    }

    @Test
    public void testLiveInsert() throws InterruptedException {

        ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OLiveQueryTest");
        db.activateOnCurrentThread();

        if (!db.exists()) {
            db.create();
        }
        try {
            db.getMetadata().getSchema().createClass("test");
            db.getMetadata().getSchema().createClass("test2");
            MyLiveQueryListener listener = new MyLiveQueryListener();

            OResultSet<ODocument> tokens = db.query(new OLiveQuery<ODocument>("live select _id as  id , surname.toUpperCase() as upName  from test where name='foo'", listener));
            Assert.assertEquals(tokens.size(), 1);

            ODocument tokenDoc = tokens.get(0);
            Integer token = tokenDoc.field("token");
            Assert.assertNotNull(token);

            db.command(new OCommandSQL("insert into test set name = 'foo', surname = 'bar', _id=1")).execute();
            db.command(new OCommandSQL("insert into test set name = 'foo', surname = 'baz', _id=2")).execute();
            db.command(new OCommandSQL("insert into test set name = 'not foo', surname = 'bat', _id=3")).execute();
            db.command(new OCommandSQL("insert into test2 set name = 'foo'")).execute();
            latch.await(150 , TimeUnit.MILLISECONDS);
            Assert.assertEquals(listener.rowCount, 2);

            db.command(new OCommandSQL("update test set name = 'foo2' where  _id=2")).execute();
            latch.await(150 , TimeUnit.MILLISECONDS);
            Assert.assertEquals(listener.rowCount, 1);
            db.command(new OCommandSQL("update  test set name = 'foo', surname = 'buzzzz' where  _id=3")).execute();
            latch.await(150 , TimeUnit.MILLISECONDS);
            Assert.assertEquals(listener.rowCount, 2);
            db.command(new OCommandSQL("update test set  surname = 'Looks Fantastic' where  _id=1")).execute();
            latch.await(150 , TimeUnit.MILLISECONDS);
            Assert.assertEquals(listener.rowCount, 2);

            db.command(new OCommandSQL("live unsubscribe " + token)).execute();

            db.command(new OCommandSQL("insert into test set name = 'foo', surname = 'bax'")).execute();
            db.command(new OCommandSQL("insert into test2 set name = 'foo'"));
            db.command(new OCommandSQL("insert into test set name = 'foo', surname = 'baz'")).execute();

            Assert.assertEquals(listener.rowCount, 2);
        } catch (Exception ex) {
            System.out.println(ex);

        } finally {

            db.drop();
        }
    }

    @Test
    public void testLiveInsertOnCluster() throws InterruptedException {
        ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OLiveQueryTest");
        db.activateOnCurrentThread();
        db.create();
        try {
            OClass clazz = db.getMetadata().getSchema().createClass("test");

            int defaultCluster = clazz.getDefaultClusterId();
            OCluster cluster = db.getStorage().getClusterById(defaultCluster);

            MyLiveQueryListener listener = new MyLiveQueryListener();

            db.query(new OLiveQuery<ODocument>("live select from cluster:" + cluster.getName() + " where name='foo'", listener));

            db.command(new OCommandSQL("insert into cluster:" + cluster.getName() + " set name = 'foo', surname = 'bar', _id =2"))
                    .execute();
            latch.await(150 , TimeUnit.MILLISECONDS);
            Assert.assertEquals(1, listener.rowCount);
            
            db.command(new OCommandSQL("update   cluster:" + cluster.getName() + " set name = 'not foo' where _id=2"))
                    .execute();
            latch.await(150 , TimeUnit.MILLISECONDS);
            Assert.assertEquals(0, listener.rowCount);
            db.command(new OCommandSQL("update   cluster:" + cluster.getName() + " set name = 'foo' , surname='foo backs so it  is new create '  where _id=2")).execute();
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Assert.assertEquals(listener.rowCount,1);
        } finally {
            db.drop();
        }
    }

}
