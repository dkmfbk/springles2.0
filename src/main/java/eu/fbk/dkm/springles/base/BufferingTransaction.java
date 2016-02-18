package eu.fbk.dkm.springles.base;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;

import eu.fbk.dkm.internal.util.Contexts;
import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;

/**
 * A <tt>Transaction</tt> decorator providing buffering capability.
 * <p>
 * This wrapper keeps a buffer of statements. Adding statements causes the buffer to be filled;
 * when it is full, buffered statements are 'flushed', i.e., added to the underlying transaction
 * in a single operation, with a gain in performances. Similarly, removed statements are
 * registered in the buffered, and 'flushed', i.e., removed from the underlying transaction once
 * the buffer is full. Buffered statements are associated to a list of contexts (the one specified
 * in <tt>add</tt> and <tt>remove</tt> methods). More in details, flushing occurs when:
 * <ul>
 * <li>there is a switch from adding to removing statements and vice-versa;</li>
 * <li>an <tt>add</tt> or <tt>remove</tt> operation is issued with a different list of contexts
 * with respect to the contexts of buffered statements;</li>
 * <li>a read operation is issued (methods <tt>query</tt>, <tt>getContextIDs</tt>,
 * <tt>getStatements</tt>, <tt>hasStatement</tt> and <tt>size</tt>);</li>
 * <li>an update operation is issued, as they may read the repository contents too (methods
 * <tt>update</tt>);</li>
 * <li>an inference-related method is called (<tt>getClosureStatus</tt>, <tt>updateClosure</tt>,
 * <tt>clearClosure</tt>);</li>
 * <li>when <tt>execute()</tt> is executed in read-only mode or requiring the computation of
 * closure.</li>
 * </ul>
 * Finally, the buffer is cleared every time <tt>reset</tt> is called, as there is no need to
 * flush buffered statements after that operation.
 * </p>
 * <p>
 * NOTE: this wrapper must not be used in auto-commit mode, as it merges together multiple
 * <tt>add</tt> and <tt>remove</tt> operation and hence breaks the contract 'one user operation =
 * one transaction'.
 * </p>
 */
final class BufferingTransaction extends ForwardingTransaction
{

    // IMPLEMENTATION NOTES
    //
    // Buffering logic is implemented in methods flush(), add() and remove(). A bit of duplication
    // occurs between the last two, but a better solution guaranteeing the same simplicity (3
    // methods) and logging details was not found. Note also that for performance reasons, only a
    // quick identity check of context arrays is performed, checking the empty context array as
    // special case

    /** Shared log object. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BufferingTransaction.class);

    /** Buffer capacity. Each time it is reached, the buffer is flushed to the repository. */
    private static final int BATCH_SIZE = 1024;

    /** The underlying transaction this wrapper delegates to. */
    private final Transaction delegate;

    /** A list of buffered statements, in the order they were added or removed. */
    private final List<Statement> statements;

    /** The contexts associated to buffered statements. */
    private Resource[] contexts;

    /** Flag <tt>true</tt> if buffered statements are being added to the repository. */
    private boolean adding;

    /**
     * Creates a new instance wrapping the supplied <tt>Transaction</tt>
     * 
     * @param delegate
     *            the wrapped transaction, not null
     */
    public BufferingTransaction(final Transaction delegate)
    {
        Preconditions.checkNotNull(delegate);

        this.delegate = delegate;
        this.statements = Lists.newArrayListWithCapacity(BATCH_SIZE);
        this.contexts = Contexts.UNSPECIFIED;
        this.adding = false;
    }

    /**
     * {@inheritDoc} Returns the underlying transaction set at construction time.
     */
    @Override
    protected Transaction delegate()
    {
        return this.delegate;
    }

    /**
     * Forces flushing buffered statements to wrapped <tt>Transaction</tt>. This is a NOP if the
     * buffer is empty.
     * 
     * @throws RepositoryException
     *             on failure
     */
    private synchronized void flush() throws RepositoryException
    {
        final int size = this.statements.size();
        if (size == 0) {
            return;
        } else if (this.adding) {
            LOGGER.debug("Flushing buffer: adding {} statements to repository", size);
            delegate().add(this.statements, this.contexts);
        } else {
            LOGGER.debug("Flushing buffer: removing {} statements from repository", size);
            delegate().remove(this.statements, this.contexts);
        }
        this.statements.clear();
    }

    /**
     * {@inheritDoc} Flushes and delegates.
     */
    @Override
    public void query(final QuerySpec<?> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode, final int timeout,
            final Object handler) throws QueryEvaluationException, RepositoryException
    {
        flush();
        delegate().query(query, dataset, bindings, mode, timeout, handler);
    }

    /**
     * {@inheritDoc} Flushes and delegates.
     */
    @Override
    public <T> T query(final QuerySpec<T> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode, final int timeout)
            throws QueryEvaluationException, RepositoryException
    {
        flush();
        return delegate().query(query, dataset, bindings, mode, timeout);
    }

    /**
     * {@inheritDoc} Flushes and delegates.
     */
    @Override
    public <T> T query(final URI queryURI, final QueryType<T> queryType, final InferenceMode mode,
            final Object... parameters) throws QueryEvaluationException, RepositoryException
    {
        flush();
        return delegate().query(queryURI, queryType, mode, parameters);
    }

    /**
     * {@inheritDoc} Flushes and delegates.
     */
    @Override
    public CloseableIteration<? extends Resource, RepositoryException> getContextIDs(
            final InferenceMode mode) throws RepositoryException
    {
        flush();
        return delegate().getContextIDs(mode);
    }

    /**
     * {@inheritDoc} Flushes and delegates.
     */
    @Override
    public CloseableIteration<? extends Statement, RepositoryException> getStatements(
            @Nullable final Resource subj, @Nullable final URI pred, @Nullable final Value obj,
            final InferenceMode mode, final Resource... contexts) throws RepositoryException
    {
        flush();
        return delegate().getStatements(subj, pred, obj, mode, contexts);
    }

    /**
     * {@inheritDoc} Flushes and delegates.
     */
    @Override
    public boolean hasStatement(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final InferenceMode mode, final Resource... contexts)
            throws RepositoryException
    {
        flush();
        return delegate().hasStatement(subj, pred, obj, mode, contexts);
    }

    /**
     * {@inheritDoc} Flushes and delegates.
     */
    @Override
    public long size(final InferenceMode mode, final Resource... contexts)
            throws RepositoryException
    {
        flush();
        return delegate().size(mode, contexts);
    }

    /**
     * {@inheritDoc} Flushes and delegates.
     */
    @Override
    public void update(final UpdateSpec update, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode)
            throws UpdateExecutionException, RepositoryException
    {
        flush();
        delegate().update(update, dataset, bindings, mode);
    }

    /**
     * {@inheritDoc} Flushes and delegates.
     */
    @Override
    public void update(final URI updateURI, final InferenceMode mode, final Object... parameters)
            throws UpdateExecutionException, RepositoryException
    {
        flush();
        delegate().update(updateURI, mode, parameters);
    }

    /**
     * {@inheritDoc} Current buffer content is flushed if a remove / add switch or a change in
     * contexts is detected. If the number of supplied statements is known (a <tt>Collection</tt>
     * is supplied) then either they are totally buffered, or they are propagated together with
     * buffered contents to the wrapped transaction if the combined size exceeds the buffer
     * capacity. Otherwise, supplied statements are buffered one at a time and flushing occurs
     * each time the buffer capacity is reached.
     */
    @Override
    public synchronized void add(final Iterable<? extends Statement> statements,
            final Resource... contexts) throws RepositoryException
    {
        Preconditions.checkNotNull(statements); // fail-fast
        Preconditions.checkNotNull(contexts); // fail-fast

        if (Iterables.isEmpty(statements)) {
            return; // nop
        }

        final boolean sameContexts = this.contexts == contexts || this.contexts.length == 0
                && contexts.length == 0;

        if (!this.statements.isEmpty()) {
            if (!this.adding) {
                LOGGER.debug("Flushing buffered statements due to remove -> add switch: "
                        + "removing {} statements from repository", this.statements.size());
                delegate().remove(this.statements, this.contexts);
                this.statements.clear();

            } else if (!sameContexts) {
                LOGGER.debug("Flushing buffered statements due to change of contexts: "
                        + "adding {} statements to repository", this.statements.size());
                delegate().add(this.statements, this.contexts);
                this.statements.clear();
            }
        }

        this.adding = false;
        if (!sameContexts) {
            this.contexts = contexts.length == 0 ? contexts : contexts.clone();
        }

        if (statements instanceof Collection<?>) {
            final int size = this.statements.size();
            final int delta = ((Collection<?>) statements).size();
            final int total = size + delta;
            if (total < BATCH_SIZE) {
                LOGGER.debug("Buffered {} statements for addition; buffer contains {} statements",
                        delta, total);
                this.statements.addAll((Collection<? extends Statement>) statements);

            } else if (size == 0) {
                LOGGER.debug("Number of statements to add ({}) exceeds buffer size: "
                        + "propagating without buffering", delta);
                delegate().add(statements, contexts);

            } else {
                LOGGER.debug("Flushing buffer together with specified statements: "
                        + "adding {} statements overall", total);
                delegate().add(Iterables.concat(this.statements, statements), this.contexts);
                this.statements.clear();
            }
            return;

        } else {
            LOGGER.debug("Adding unprecised number of statements");
            for (final Statement statement : statements) {
                this.statements.add(statement);
                if (this.statements.size() == BATCH_SIZE) {
                    LOGGER.debug("Flushing buffer due to capacity reached: "
                            + "adding {} statements to repository", this.statements.size());
                    delegate().add(this.statements, this.contexts);
                    this.statements.clear();
                }
            }
            LOGGER.debug("Buffer contains {} statements to be added", this.statements.size());
        }
    }

    /**
     * {@inheritDoc} The method proceeds as described for {@link #add(Iterable, Resource...)}.
     */
    @Override
    public synchronized void remove(final Iterable<? extends Statement> statements,
            final Resource... contexts) throws RepositoryException
    {
        Preconditions.checkNotNull(statements); // fail-fast
        Preconditions.checkNotNull(contexts); // fail-fast

        if (Iterables.isEmpty(statements)) {
            return; // nop
        }

        final boolean sameContexts = this.contexts == contexts || this.contexts.length == 0
                && contexts.length == 0;

        if (!this.statements.isEmpty()) {
            if (this.adding) {
                LOGGER.debug("Flushing buffered statements due to add -> remove switch: "
                        + "adding {} statements from repository", this.statements.size());
                delegate().add(this.statements, this.contexts);
                this.statements.clear();

            } else if (!sameContexts) {
                LOGGER.debug("Flushing buffered statements due to change of contexts: "
                        + "removing {} statements to repository", this.statements.size());
                delegate().remove(this.statements, this.contexts);
                this.statements.clear();
            }
        }

        this.adding = true;
        if (!sameContexts) {
            this.contexts = contexts.length == 0 ? contexts : contexts.clone();
        }

        if (statements instanceof Collection<?>) {
            final int size = this.statements.size();
            final int delta = ((Collection<?>) statements).size();
            final int total = size + delta;
            if (total < BATCH_SIZE) {
                LOGGER.debug("Buffered {} statements for removal; buffer contains {} statements",
                        delta, total);
                this.statements.addAll((Collection<? extends Statement>) statements);

            } else if (size == 0) {
                LOGGER.debug("Number of statements to remove ({}) exceeds buffer size: "
                        + "propagating without buffering", delta);
                delegate().remove(statements, contexts);

            } else {
                LOGGER.debug("Flushing buffer together with specified statements: "
                        + "removing {} statements overall", total);
                delegate().remove(Iterables.concat(this.statements, statements), this.contexts);
                this.statements.clear();
            }
            return;

        } else {
            LOGGER.debug("Removing unprecised number of statements");
            for (final Statement statement : statements) {
                this.statements.add(statement);
                if (this.statements.size() == BATCH_SIZE) {
                    LOGGER.debug("Flushing buffer due to capacity reached: "
                            + "removing {} statements to repository", this.statements.size());
                    delegate().remove(this.statements, this.contexts);
                    this.statements.clear();
                }
            }
            LOGGER.debug("Buffer contains {} statements to be removed", this.statements.size());
        }
    }

    /**
     * {@inheritDoc} Delegates to {@link #remove(Iterable, Resource...)} if there are no
     * wildcards, so to exploit buffering, otherwise flushes and delegates.
     */
    @Override
    public void remove(@Nullable final Resource subject, @Nullable final URI predicate,
            @Nullable final Value object, final Resource... contexts) throws RepositoryException
    {
        if (subject != null && predicate != null && object != null) {
            remove(Collections.singleton(ValueFactoryImpl.getInstance().createStatement(subject,
                    predicate, object)), contexts);
        } else {
            flush();
            delegate().remove(subject, predicate, object, contexts);
        }
    }

    /**
     * {@inheritDoc} Flushes and delegates.
     */
    @Override
    public ClosureStatus getClosureStatus() throws RepositoryException
    {
        flush();
        return delegate().getClosureStatus();
    }

    /**
     * {@inheritDoc} Flushes and delegates.
     */
    @Override
    public void updateClosure() throws RepositoryException
    {
        flush();
        delegate().updateClosure();
    }

    /**
     * {@inheritDoc} Flushes and delegates.
     */
    @Override
    public void clearClosure() throws RepositoryException
    {
        flush();
        delegate().clearClosure();
    }

    /**
     * {@inheritDoc} Delegates, discarding any buffered statement (no need to flush changes).
     */
    @Override
    public void reset() throws RepositoryException
    {
        this.statements.clear();
        delegate().reset();
    }

    /**
     * {@inheritDoc} Flushes the buffer if the operation is executed in read-only mode (as it
     * would not be possible to flush after the operation starts) or closure is needed (as closure
     * computation may occur), then delegates.
     */
    @Override
    public <T, E extends Exception> T execute(final Operation<T, E> operation,
            final boolean writeOperation, final boolean closureNeeded) throws E,
            RepositoryException
    {
        if (!writeOperation || closureNeeded) {
            flush();
        }
        return delegate().execute(operation, writeOperation, closureNeeded);
    }

    /**
     * {@inheritDoc} Flushes and delegates.
     */
    @Override
    public void end(final boolean commit) throws RepositoryException
    {
        flush();
        delegate().end(commit);
    }

}
