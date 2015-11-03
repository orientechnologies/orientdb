package org.apache.tinkerpop.gremlin.orientdb;

import org.apache.tinkerpop.gremlin.structure.util.AbstractTransaction;

public class OrientTransaction extends AbstractTransaction {

    protected OrientGraph graph;

    public OrientTransaction(OrientGraph graph) {
        super(graph);
        this.graph = graph;
        graph.begin();
    }

    @Override
    public boolean isOpen() {
        return !graph.isClosed();
    }

    @Override
    protected void doOpen() {
        graph.begin();
    }

    @Override
    protected void doCommit() throws TransactionException {
        graph.commit();
    }

    @Override
    protected void doRollback() throws TransactionException {
        graph.rollback();
    }

}
