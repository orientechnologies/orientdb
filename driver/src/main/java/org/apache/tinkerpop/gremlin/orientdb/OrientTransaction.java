package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.tx.OTransaction;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;

public class OrientTransaction extends AbstractThreadLocalTransaction {


    protected OrientGraph graph;

    public OrientTransaction(OrientGraph graph) {
        super(graph);
        this.graph = graph;
    }

    @Override
    public boolean isOpen() {
        return this.tx().isActive();
    }

    @Override
    protected void doOpen() {
        this.db().begin();
    }

    @Override
    protected void doCommit() throws TransactionException {
        this.db().commit();
    }

    @Override
    protected void doClose() {
        super.doClose();
    }

    @Override
    protected void doReadWrite() {
        super.doReadWrite();
    }

    @Override
    protected void fireOnCommit() {
        super.fireOnCommit();
    }

    @Override
    protected void fireOnRollback() {
        super.fireOnRollback();
    }

    @Override
    protected void doRollback() throws TransactionException {
        this.db().rollback();
    }

    protected OTransaction tx() {
        return graph.getRawDatabase().getTransaction();
    }

    protected ODatabaseDocument db() {
        return graph.getRawDatabase();
    }
}
