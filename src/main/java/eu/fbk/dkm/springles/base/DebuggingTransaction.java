package eu.fbk.dkm.springles.base;

import java.util.Arrays;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

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
import org.slf4j.Logger;

import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;

final class DebuggingTransaction extends ForwardingTransaction
{

    private final Logger logger;

    private final Transaction delegate;

    private volatile boolean closed;

    public DebuggingTransaction(final Transaction delegate, final Logger logger)
    {
        this.delegate = delegate;
        this.logger = logger;
        this.closed = false;
    }

    @Override
    protected Transaction delegate()
    {
        return this.delegate();
    }

    @Override
    public String getID()
    {
        return this.delegate.getID();
    }

    @Override
    public ValueFactory getValueFactory()
    {
        return this.delegate.getValueFactory();
    }

    @Override
    public String getNamespace(final String prefix) throws RepositoryException
    {
        Preconditions.checkNotNull(prefix);
        Preconditions.checkState(!this.closed);

        this.logger.info("[{}] Retrieving namespace for prefix {}", this.delegate.getID(), prefix);
        return this.delegate.getNamespace(prefix);
    }

    @Override
    public CloseableIteration<? extends Namespace, RepositoryException> //
            getNamespaces() throws RepositoryException
    {
        Preconditions.checkState(!this.closed);

        this.logger.info("[{}] Retrieving namespaces", this.delegate.getID());
        return this.delegate.getNamespaces();
    }

    @Override
    public void setNamespace(final String prefix, @Nullable final String name)
            throws RepositoryException
    {
        Preconditions.checkNotNull(prefix);
        Preconditions.checkState(!this.closed);

        if (this.logger.isInfoEnabled()) {
            if (name != null) {
                this.logger.info("[" + this.delegate.getID() + "] Binding prefix " + prefix
                        + " to namespace " + name);
            } else {
                this.logger.info("[" + this.delegate.getID()
                        + "] Removing namespace binding for prefix " + prefix);
            }
        }

        this.delegate.setNamespace(prefix, name);
    }

    @Override
    public void clearNamespaces() throws RepositoryException
    {
        Preconditions.checkState(!this.closed);

        this.logger.info("[{}] Clearing namespace bindings", this.delegate.getID());
        this.delegate.clearNamespaces();
    }

    @Override
    public void query(final QuerySpec<?> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode, final int timeout,
            final Object handler) throws QueryEvaluationException, RepositoryException
    {
        Preconditions.checkNotNull(query);
        Preconditions.checkNotNull(mode);
        Preconditions.checkNotNull(handler);
        Preconditions.checkState(!this.closed);

        if (this.logger.isInfoEnabled()) {
            this.logQuery(query, dataset, bindings, timeout, handler);
        }

        this.delegate.query(query, dataset, bindings, mode, timeout, handler);
    }

    @Override
    public <T> T query(final QuerySpec<T> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode, final int timeout)
            throws QueryEvaluationException, RepositoryException
    {
        Preconditions.checkNotNull(query);
        Preconditions.checkNotNull(mode);
        Preconditions.checkState(!this.closed);

        if (this.logger.isInfoEnabled()) {
            this.logQuery(query, dataset, bindings, timeout, null);
        }

        return this.delegate.query(query, dataset, bindings, mode, timeout);
    }

    @Override
    public <T> T query(final IRI queryURI, final QueryType<T> queryType, final InferenceMode mode,
            final Object... parameters) throws QueryEvaluationException, RepositoryException
    {
        Preconditions.checkNotNull(queryURI);
        Preconditions.checkNotNull(queryType);
        Preconditions.checkNotNull(mode);
        Preconditions.checkNotNull(parameters);
        Preconditions.checkState(!this.closed);

        if (this.logger.isInfoEnabled()) {
            this.logger.info(
                    "[{}] Evaluating named query {} of type {} with parameters {}; "
                            + "inference mode is {}",
                    new Object[] { this.delegate.getID(), queryURI, queryType,
                            Arrays.toString(parameters), mode });
        }

        return this.delegate.query(queryURI, queryType, mode, parameters);
    }

    private void logQuery(final QuerySpec<?> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final int timeout, final Object handler)
    {
        final StringBuilder builder = new StringBuilder();
        builder.append("[").append(this.delegate.getID()).append("] Evaluating query")
                .append(handler == null ? " without handler" : "with handler");
        if (bindings != null) {
            builder.append(", ").append(bindings.size()).append(" bindings");
        }
        builder.append(" and timeout ").append(timeout).append(" ms");
        if (dataset != null) {
            builder.append("; dataset is\n").append(dataset);
        }
        builder.append("\n").append(query);
        this.logger.info(builder.toString());
    }

    @Override
    public CloseableIteration<? extends Resource, RepositoryException> getContextIDs(
            final InferenceMode mode) throws RepositoryException
    {
        Preconditions.checkNotNull(mode);
        Preconditions.checkState(!this.closed);

        this.logger.info("[{}] Retrieving context IDs", this.delegate.getID());

        return this.delegate.getContextIDs(mode);
    }

    @Override
    public CloseableIteration<? extends Statement, RepositoryException> getStatements(
            @Nullable final Resource subj, @Nullable final IRI pred, @Nullable final Value obj,
            final InferenceMode mode, final Resource... contexts) throws RepositoryException
    {
        Preconditions.checkNotNull(mode);
        Preconditions.checkNotNull(contexts);
        Preconditions.checkState(!this.closed);

        if (this.logger.isInfoEnabled()) {
            this.logger.info("[" + this.delegate.getID() + "] Retrieving statements matching <"
                    + subj + ", " + pred + ", " + obj + ">" + (contexts.length == 0 ? "" : //
                            " in contexts " + Arrays.toString(contexts)));
        }

        return this.delegate.getStatements(subj, pred, obj, mode, contexts);
    }

    @Override
    public boolean hasStatement(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final InferenceMode mode, final Resource... contexts)
            throws RepositoryException
    {
        Preconditions.checkNotNull(mode);
        Preconditions.checkNotNull(contexts);
        Preconditions.checkState(!this.closed);

        if (this.logger.isInfoEnabled()) {
            this.logger.info(
                    "[" + this.delegate.getID() + "] Checking existence of statements matching <"
                            + subj + ", " + pred + ", " + obj + ">" + (contexts.length == 0 ? "" : //
                                    " in contexts " + Arrays.toString(contexts)));
        }

        return this.delegate.hasStatement(subj, pred, obj, mode, contexts);
    }

    @Override
    public long size(final InferenceMode mode, final Resource... contexts)
            throws RepositoryException
    {
        Preconditions.checkNotNull(mode);
        Preconditions.checkNotNull(contexts);
        Preconditions.checkState(!this.closed);

        this.logger.info("[{}] Retrieving total number of statements", this.delegate.getID());

        return this.delegate.size(mode, contexts);
    }

    @Override
    public void update(final UpdateSpec update, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode)
            throws UpdateExecutionException, RepositoryException
    {
        Preconditions.checkNotNull(mode);
        Preconditions.checkState(!this.closed);

        if (this.logger.isInfoEnabled()) {
            this.logger.info("[" + this.delegate.getID() + "] Executing update"
                    + (bindings == null ? "" : " with " + bindings.size() + " bindings")
                    + (dataset == null ? "" : ", dataset is\n" + dataset) + "\n" + update);
        }

        this.delegate.update(update, dataset, bindings, mode);
    }

    @Override
    public void update(final IRI updateURI, final InferenceMode mode, final Object... parameters)
            throws UpdateExecutionException, RepositoryException
    {
        Preconditions.checkNotNull(updateURI);
        Preconditions.checkNotNull(mode);
        Preconditions.checkNotNull(parameters);
        Preconditions.checkState(!this.closed);

        if (this.logger.isInfoEnabled()) {
            this.logger.info(
                    "[{}] Executing named update {} with parameters {}; inference " + "mode is {}",
                    new Object[] { this.delegate.getID(), updateURI, //
                            Arrays.toString(parameters), mode });
        }

        this.delegate.update(updateURI, mode, parameters);
    }

    @Override
    public void add(final Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException
    {
        Preconditions.checkNotNull(statements);
        Preconditions.checkNotNull(contexts);
        Preconditions.checkState(!this.closed);

        if (this.logger.isInfoEnabled()) {
            this.logger.info("[" + this.delegate.getID() + "] Adding " + Iterables.size(statements)
                    + " statements" + (contexts.length == 0 ? "" : //
                            " in contexts " + Arrays.toString(contexts)));
        }

        this.delegate.add(statements, contexts);
    }

    @Override
    public void remove(final Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException
    {
        Preconditions.checkNotNull(statements);
        Preconditions.checkNotNull(contexts);
        Preconditions.checkState(!this.closed);

        if (this.logger.isInfoEnabled()) {
            this.logger.info("[" + this.delegate.getID() + "] Removing "
                    + Iterables.size(statements) + " statements" + (contexts.length == 0 ? "" : //
                            " from contexts " + Arrays.toString(contexts)));
        }

        this.delegate.remove(statements, contexts);
    }

    @Override
    public void remove(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final Resource... contexts) throws RepositoryException
    {
        Preconditions.checkNotNull(contexts);
        Preconditions.checkState(!this.closed);

        if (this.logger.isInfoEnabled()) {
            this.logger.info("[" + this.delegate.getID() + "] Removing statements matching <"
                    + subj + ", " + pred + ", " + obj + ">" + (contexts.length == 0 ? "" : //
                            " from contexts " + Arrays.toString(contexts)));
        }

        this.delegate.remove(subj, pred, obj, contexts);
    }

    @Override
    public ClosureStatus getClosureStatus() throws RepositoryException
    {
        Preconditions.checkState(!this.closed);

        return this.delegate.getClosureStatus();
    }

    @Override
    public void updateClosure() throws RepositoryException
    {
        Preconditions.checkState(!this.closed);

        this.delegate.updateClosure();
    }

    @Override
    public void clearClosure() throws RepositoryException
    {
        Preconditions.checkState(!this.closed);

        this.delegate.clearClosure();
    }

    @Override
    public void reset() throws RepositoryException
    {
        Preconditions.checkState(!this.closed);

        this.logger.info("[{}] Resetting repository", this.delegate.getID());

        this.delegate.reset();
    }

    @Override
    public <T, E extends Exception> T execute(final Operation<T, E> operation,
            final boolean writeOperation, final boolean closureNeeded)
            throws E, RepositoryException
    {
        Preconditions.checkNotNull(operation);
        Preconditions.checkState(!this.closed);

        if (this.logger.isInfoEnabled()) {
            this.logger.info("[{}] Executing operation {}, write = {}, closure needed = {}",
                    new Object[] { this.delegate.getID(), operation.getClass().getName(),
                            writeOperation, closureNeeded });
        }

        return this.delegate.execute(operation, writeOperation, closureNeeded);
    }

    @Override
    public void end(final boolean commit) throws RepositoryException
    {
        Preconditions.checkState(!this.closed);

        this.logger.info("[{}] Ending transaction, commit = {}", this.delegate.getID(), commit);

        try {
            this.delegate.end(commit);
        } finally {
            this.closed = true;
        }
    }

}
