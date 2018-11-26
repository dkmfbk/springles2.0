package eu.fbk.dkm.springles.inferencer;

import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryException;

import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.dkm.springles.base.QuerySpec;
import eu.fbk.dkm.springles.base.UpdateSpec;

// How to handle exceptions from implementations of this class:
//
// - configurationSignature: exception logged; for safety reasons, it is assumed a new
// configuration applies so closure is invalid
// - rewriteQuery: exception wrapped and reported as RepositoryException, as cannot guarantee
// completeness of results
// - rewriteUpdate: exception wrapped and reported as RepositoryException, as cannot guarantee
// completeness of results
// - createClosureMaintainer: no maintainer will be associated to the transaction, meaning every
// operation calling updateClosure() will fail

/**
 * Inference engine supporting both computation of logical closure and backward reasoning through
 * query rewriting.
 * <p>
 * Implementations of this interface must be THREAD SAFE, as it must be possible to call the
 * methods of this interface concurrently from multiple threads.
 * </p>
 *
 * @apiviz.landmark
 * @apiviz.uses eu.fbk.dkm.springles.inferencer.Inferencer.Session - - - <<create>>
 */
@ThreadSafe
public interface Inferencer
{

    void initialize(String inferredContextPrefix) throws RepositoryException;

    InferenceMode getInferenceMode();

    /**
     * Returns a string digest depending on any configuration parameter that may determine a
     * change in the entailments produced by the engine. This method serves to detect when, after
     * a change in the configuration of the inference engine (e.g., a change of configured
     * inference rules), the already computed closure becomes invalid and has thus to be
     * recomputed. This method is not supposed to throw exceptions.
     *
     * @return the computed string signature
     */
    String getConfigurationDigest();

    /**
     * Creates a new {@link Session} for the read or read/write transaction currently active.
     * Parameter <tt>writable</tt> specifies whether the underlying transaction and connection
     * object are modifiable, while parameter <tt>currentClosureStatus</tt> specifies the status
     * of the closure currently materialized in the repository.
     *
     * @param id
     *            a parameter identifying the session (e.g., for logging purposes), typically
     *            related to the transaction it is associated to
     * @param closureStatus
     *            the status of the closure currently materialized in the repository
     * @param context
     *            a context object providing read/write (if writable) access to data in the
     *            repository
     * @return the created {@link Session}
     * @throws RepositoryException
     *             in case the creation failed, for any reason
     */
    Session newSession(String id, ClosureStatus closureStatus, Context context)
            throws RepositoryException;

    void close() throws RepositoryException;

    // bound to a read/write transaction, disposed after transaction ends. read transaction cannot
    // compute/update the closure.
    //
    // how to handle exceptions:
    // - statementsAdded and statementsRemoved: exception logged and ignored (they are callbacks)
    // - rewriteQuery: exception wrapped and reported as RepositoryException, as cannot guarantee
    // completeness of results
    // - rewriteUpdate: exception wrapped and reported as RepositoryException, as cannot guarantee
    // completeness of results
    // - updateClosure: no update of closure status; throw RepositoryException either to signal
    // failure
    // of computeClosure or reject executing read/query
    // - close: if the connection is writable, as it may perform some cleanup or modification to
    // the
    // repository we do a rollback

    // write operations in inference context can be called only in updateClosure() and close()
    // when
    // committing is true.

    /**
     * <p>
     * Implementations of this interface do not have to be thread safe. However, it must be
     * possible for different threads to access an instance of the interface, not concurrently.
     * </p>
     *
     * @apiviz.uses eu.fbk.dkm.springles.inferencer.Inferencer.Context - - - <<call>>
     * @apiviz.uses eu.fbk.dkm.springles.base.QuerySpec - - - <<rewrite>>
     * @apiviz.uses eu.fbk.dkm.springles.base.UpdateSpec - - - <<rewrite>>
     */
    @NotThreadSafe
    public interface Session
    {

        // Rationale for contexts field: without it, will have to expand statements supplied to
        // add() and remove() methods, even if the listener does nothing with them. Better to
        // postpone expansion (use RepositoryUtils.expand() method for that), so it can be avoided
        // if unnecessary.

        // following methods are not allowed to update the closure (unless update closure is in
        // progress); they may however write auxiliary data (invisible to user queries) to the
        // repository.

        void statementsAdded(@Nullable Iterable<? extends Statement> statements,
                Resource... contexts) throws RepositoryException;

        void statementsRemoved(@Nullable Iterable<? extends Statement> statements,
                Resource... contexts) throws RepositoryException;

        void statementsCleared(boolean onlyClosure) throws RepositoryException;

        /**
         * Rewrites a query so to implement query-time backward reasoning. The query to rewrite is
         * supplied already parsed and thus expressed in the Sesame algebra; this guarantee that
         * the expression is syntactically correct and permits to abstract from the actual query
         * language used to express the query (e.g., SPARQL or SERQL). The rewriting, if any,
         * should modify the supplied parsed query object, by either modifying the algebraic
         * expression or the dataset over which the query should be evaluated.
         *
         * @param query
         *            the query to rewrite
         * @return the rewritten query, possibly the same input object; a <tt>null</tt> return
         *         value signal that the query has not to be evaluated as it is not expected to
         *         produce results.
         * @throws RepositoryException
         *             in case rewriting failed for any other reason
         */
        <T> QuerySpec<T> rewriteQuery(QuerySpec<T> query, ClosureStatus closureStatus,
                boolean forwardInferenceEnabled) throws RepositoryException; // concurrent
        // access

        /**
         * Rewrites an update so to implement query-time backward reasoning. Update expressions
         * (e.g., SPARQL modify) may require querying data in the repository, in which case
         * rewriting has to occur so for query results to be complete. The update expression to
         * rewrite is supplied already parsed and consists in a list of syntactically correct
         * commands, in the Sesame algebra, each possibly associated to a dataset. The rewriting,
         * if any, should modify the supplied parsed update object, by either modifying the
         * algebraic expressions or the datasets over which the update command sequence should be
         * evaluated.
         *
         * @param update
         *            the update to rewrite
         * @return the rewritten update, possibly the same input object; a <tt>null</tt> return
         *         value signal that the update has not to be evaluated as it is not expected to
         *         produce any modification.
         * @throws RepositoryException
         *             in case rewriting failed for any other reason
         */
        UpdateSpec rewriteUpdate(UpdateSpec update, ClosureStatus closureStatus,
                boolean forwardInferenceEnabled) throws RepositoryException;

        // invoked only when closurestatus is incomplete

        // can both add and delete statements inside inferred graphs under the control of the
        // inference engine

        // invoked only when closurestatus is incomplete

        // can both add and delete statements inside inferred graphs under the control of the
        // inference engine

        void updateClosure(ClosureStatus closureStatus) throws RepositoryException; // writable

        // if the connection is writable, can write/modify auxiliary data to the repository.

        void close(boolean committing) throws RepositoryException; // writable if committing

    }

    public interface Context
    {

        @Nullable
        ScheduledExecutorService getScheduler();

        ValueFactory getValueFactory();

        <T> T query(QuerySpec<T> query, @Nullable Dataset dataset, @Nullable BindingSet bindings,
                boolean includeClosure, int timeout)
                throws MalformedQueryException, QueryEvaluationException, RepositoryException;

        CloseableIteration<? extends Resource, RepositoryException> getContextIDs(
                boolean includeClosure) throws RepositoryException;

        CloseableIteration<? extends Statement, RepositoryException> getStatements(
                @Nullable Resource subj, @Nullable IRI pred, @Nullable Value obj,
                boolean includeClosure, Resource... contexts) throws RepositoryException;

        boolean hasStatement(@Nullable Resource subj, @Nullable IRI pred, @Nullable Value obj,
                boolean includeClosure, Resource... contexts) throws RepositoryException;

        long size(boolean includeClosure, Resource... contexts) throws RepositoryException;

        void addInferred(Iterable<? extends Statement> statements, final Resource... contexts)
                throws RepositoryException;

        void removeInferred(Iterable<? extends Statement> statements, final Resource... contexts)
                throws RepositoryException;

        void removeInferred(@Nullable Resource subject, @Nullable IRI predicate,
                @Nullable Value object, Resource... contexts) throws RepositoryException;

    }

}
