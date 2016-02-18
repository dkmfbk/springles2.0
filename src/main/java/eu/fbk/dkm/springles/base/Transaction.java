package eu.fbk.dkm.springles.base;

import javax.annotation.Nullable;

import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;

import info.aduna.iteration.CloseableIteration;

import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;

// all input parameters are not changed; output parameters are totally under control of caller and
// are not changed by the transaction after they are returned; note that map returned by
// getNamespaceMap is immutable

// assumption: multiple calls to method close() of returned iterations should be ignored

// methods getID and getValueFactory are always available, even after the transaction ended

/**
 * Transaction abstraction.
 * 
 * @apiviz.landmark
 * @apiviz.uses eu.fbk.dkm.springles.base.Transaction.Operation - - <<auxiliary>>
 * @apiviz.uses eu.fbk.dkm.springles.base.QuerySpec
 * @apiviz.uses eu.fbk.dkm.springles.base.UpdateSpec
 * @apiviz.uses eu.fbk.dkm.springles.base.QueryType
 */
public interface Transaction
{

    // Constant properties

    String getID();

    ValueFactory getValueFactory();

    // Namespace management

    String getNamespace(final String prefix) throws RepositoryException;

    CloseableIteration<? extends Namespace, RepositoryException> getNamespaces()
            throws RepositoryException;

    void setNamespace(String prefix, @Nullable String name) throws RepositoryException;

    void clearNamespaces() throws RepositoryException;

    // Statement read

    void query(QuerySpec<?> query, @Nullable Dataset dataset, @Nullable BindingSet bindings,
            InferenceMode mode, int timeout, Object handler) throws QueryEvaluationException,
            RepositoryException; // RDFHandler or TupleQueryResultHandler

    <T> T query(QuerySpec<T> query, @Nullable Dataset dataset, @Nullable BindingSet bindings,
            InferenceMode mode, int timeout) throws QueryEvaluationException, RepositoryException;

    <T> T query(URI queryURI, QueryType<T> queryType, InferenceMode mode, Object... parameters)
            throws QueryEvaluationException, RepositoryException;

    CloseableIteration<? extends Resource, RepositoryException> getContextIDs(InferenceMode mode)
            throws RepositoryException;

    CloseableIteration<? extends Statement, RepositoryException> getStatements(
            @Nullable Resource subj, @Nullable URI pred, @Nullable Value obj, InferenceMode mode,
            Resource... contexts) throws RepositoryException;

    boolean hasStatement(@Nullable Resource subj, @Nullable URI pred, @Nullable Value obj,
            InferenceMode mode, Resource... contexts) throws RepositoryException;

    long size(InferenceMode mode, Resource... contexts) throws RepositoryException;

    // Statement modification

    void update(UpdateSpec update, @Nullable Dataset dataset, @Nullable BindingSet bindings,
            final InferenceMode mode) throws UpdateExecutionException, RepositoryException;

    void update(URI updateURI, InferenceMode mode, Object... parameters)
            throws UpdateExecutionException, RepositoryException;

    void add(Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException;

    void remove(Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException;

    void remove(@Nullable Resource subject, @Nullable URI predicate, @Nullable Value object,
            Resource... contexts) throws RepositoryException;

    // Closure management

    ClosureStatus getClosureStatus() throws RepositoryException;

    void updateClosure() throws RepositoryException;

    void clearClosure() throws RepositoryException;

    // Resetting

    void reset() throws RepositoryException;

    // Custom operation support. Note: exclusive lock; iterations opened as part of the operation
    // MUST be closed before the operation ends

    <T, E extends Exception> T execute(Operation<T, E> operation, boolean writeOperation,
            boolean closureNeeded) throws E, RepositoryException;

    // Commit / rollback

    void end(boolean commit) throws RepositoryException;

    // Custom operation

    public interface Operation<T, E extends Exception>
    {

        T execute() throws E, RepositoryException;

    }

}
