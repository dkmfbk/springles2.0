package eu.fbk.dkm.springles;

import org.openrdf.model.Resource;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;

/**
 * Extension of <tt>RepositoryConnection</tt> with additional inference control.
 * 
 * @apiviz.landmark
 * @apiviz.has eu.fbk.dkm.springles.InferenceMode
 * @apiviz.has eu.fbk.dkm.springles.TransactionMode
 * @apiviz.has eu.fbk.dkm.springles.ClosureStatus
 */
public interface SpringlesConnection extends RepositoryConnection
{

    String getID();

    @Override
    SpringlesRepository getRepository();

    // default: supported inference mode
    InferenceMode getInferenceMode() throws RepositoryException;

    // can only restrict supported inference mode
    void setInferenceMode(InferenceMode mode) throws RepositoryException;

    // default = WRITABLE_AUTO_CLOSURE if repository writable, READ_ONLY otherwise
    TransactionMode getTransactionMode() throws RepositoryException;

    void setTransactionMode(TransactionMode mode) throws RepositoryException;

    // Extended versions of Repository methods

    RepositoryResult<Resource> getContextIDs(boolean includeInferred) throws RepositoryException;

    long size(boolean includeInferred, Resource... contexts) throws RepositoryException;

    boolean isEmpty(boolean includeInferred) throws RepositoryException;

    void reset() throws RepositoryException;

    // Inference management

    ClosureStatus getClosureStatus() throws RepositoryException;

    // error if transaction not writable, NOP if forward inference unsupported
    void updateClosure() throws RepositoryException;

    // error if transaction not writable
    void clearClosure() throws RepositoryException;

}
