package eu.fbk.dkm.springles.base;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.repository.RepositoryException;

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
            InferenceMode mode, int timeout, Object handler)
            throws QueryEvaluationException, RepositoryException; // RDFHandler or
                                                                  // TupleQueryResultHandler

    <T> T query(QuerySpec<T> query, @Nullable Dataset dataset, @Nullable BindingSet bindings,
            InferenceMode mode, int timeout) throws QueryEvaluationException, RepositoryException;

    <T> T query(IRI queryURI, QueryType<T> queryType, InferenceMode mode, Object... parameters)
            throws QueryEvaluationException, RepositoryException;

    CloseableIteration<? extends Resource, RepositoryException> getContextIDs(InferenceMode mode)
            throws RepositoryException;

    CloseableIteration<? extends Statement, RepositoryException> getStatements(
            @Nullable Resource subj, @Nullable IRI pred, @Nullable Value obj, InferenceMode mode,
            Resource... contexts) throws RepositoryException;

    boolean hasStatement(@Nullable Resource subj, @Nullable IRI pred, @Nullable Value obj,
            InferenceMode mode, Resource... contexts) throws RepositoryException;

    long size(InferenceMode mode, Resource... contexts) throws RepositoryException;

    // Statement modification

    void update(UpdateSpec update, @Nullable Dataset dataset, @Nullable BindingSet bindings,
            final InferenceMode mode) throws UpdateExecutionException, RepositoryException;

    void update(IRI updateURI, InferenceMode mode, Object... parameters)
            throws UpdateExecutionException, RepositoryException;

    void add(Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException;

    void remove(Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException;

    void remove(@Nullable Resource subject, @Nullable IRI predicate, @Nullable Value object,
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
