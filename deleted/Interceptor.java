package eu.fbk.dkm.springles.store;

import org.openrdf.repository.RepositoryException;

import eu.fbk.dkm.springles.TransactionMode;
import eu.fbk.dkm.springles.base.Transaction;

/**
 * Extension hook for store-side decoration of <tt>Transaction</tt>s.
 * 
 * @apiviz.uses eu.fbk.dkm.springles.base.Transaction - - - <<decorates>>
 */
public interface Interceptor
{

    void initialize() throws RepositoryException;

    Transaction intercept(Transaction transaction, TransactionMode transactionMode)
            throws RepositoryException;

    void close() throws RepositoryException;

}
