package com.orientechnologies.website.interceptor;

import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.events.EventManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;

/**
 * Created by Enrico Risa on 17/11/14.
 */
public class OrientDbTransactionManager extends AbstractPlatformTransactionManager implements ResourceTransactionManager {

    /**
     * The logger.
     */
    private static Logger log = LoggerFactory.getLogger(OrientDbTransactionManager.class);
    @Autowired
    private OrientDBFactory factory;

    @Autowired
    protected EventManager eventManager;

    @Override
    protected Object doGetTransaction() throws TransactionException {

        OrientTransaction transaction = new OrientTransaction();

        transaction.setGraph(factory.getGraph());

        return transaction;
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition transactionDefinition) throws TransactionException {

        OrientTransaction tx = (OrientTransaction) transaction;

        log.debug("beginning transaction, db.hashCode() = {}", tx.getDatabase().hashCode());
        if (!tx.getDatabase().getTransaction().isActive())
            tx.getDatabase().begin();
    }

    @Override
    protected void doCommit(DefaultTransactionStatus defaultTransactionStatus) throws TransactionException {
        OrientTransaction tx = (OrientTransaction) defaultTransactionStatus.getTransaction();

        log.debug("committing transaction, db.hashCode() = {}", tx.getDatabase().hashCode());
        tx.getDatabase().commit();
        eventManager.fireEvents();
    }

    @Override
    protected void doRollback(DefaultTransactionStatus defaultTransactionStatus) throws TransactionException {
        OrientTransaction tx = (OrientTransaction) defaultTransactionStatus.getTransaction();
        tx.getDatabase().rollback();
    }

    @Override
    public Object getResourceFactory() {
        return null;
    }

    public void setGraphFactory(OrientDBFactory graphFactory) {
        this.factory = graphFactory;
    }
}
