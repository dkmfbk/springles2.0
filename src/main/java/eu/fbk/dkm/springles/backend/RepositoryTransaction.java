package eu.fbk.dkm.springles.backend;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.sail.SailException;

import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.dkm.springles.base.QuerySpec;
import eu.fbk.dkm.springles.base.QueryType;
import eu.fbk.dkm.springles.base.UpdateSpec;

public class RepositoryTransaction extends AbstractBackendTransaction
{

    private final RepositoryConnection connection;

    public RepositoryTransaction(final String id, final RepositoryConnection connection)
    {
        super(id, connection.getValueFactory());
        this.connection = connection;
        try {
            this.connection.begin();
        } catch (final SailException e) {
            throw new RuntimeException(e);
        }
    }

    protected final RepositoryConnection getConnection()
    {
        return this.connection;
    }

    @Override
    public synchronized String getNamespace(final String prefix) throws RepositoryException
    {
        return this.connection.getNamespace(prefix);
    }

    @Override
    public synchronized CloseableIteration<Namespace, RepositoryException> getNamespaces()
            throws RepositoryException
    {
        return this.connection.getNamespaces();
    }

    @Override
    public synchronized void setNamespace(final String prefix, @Nullable final String name)
            throws RepositoryException
    {
        if (name != null) {
            this.connection.setNamespace(prefix, name);
        } else {
            this.connection.removeNamespace(prefix);
        }
    }

    @Override
    public synchronized void clearNamespaces() throws RepositoryException
    {
        this.connection.clearNamespaces();
    }

    @Override
    public void query(final QuerySpec<?> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode, final int timeout,
            final Object handler) throws QueryEvaluationException, RepositoryException
    {
        final Query preparedQuery = this.prepareQuery(query, dataset, bindings, timeout);

        final QueryType<?> queryType = query.getType();

        try {
            if (queryType == QueryType.TUPLE) {
                ((TupleQuery) preparedQuery).evaluate((TupleQueryResultHandler) handler);
            } else if (queryType == QueryType.GRAPH) {
                ((GraphQuery) preparedQuery).evaluate((RDFHandler) handler);
            } else if (queryType == QueryType.BOOLEAN) {
                throw new IllegalArgumentException("Handlers unsupported for boolean queries");
            } else {
                throw new Error("Unexpected query type " + queryType);
            }

        } catch (final RDFHandlerException ex) {
            throw new RepositoryException(ex.getMessage(), ex);
        } catch (final TupleQueryResultHandlerException ex) {
            throw new RepositoryException(ex.getMessage(), ex);
        }
    }

    @Override
    public <T> T query(final QuerySpec<T> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode, final int timeout)
            throws QueryEvaluationException, RepositoryException
    {
        final Query preparedQuery = this.prepareQuery(query, dataset, bindings, timeout);

        final QueryType<T> queryType = query.getType();
        final Class<T> resultClass = queryType.getResultClass();

        if (queryType == QueryType.BOOLEAN) {
            return resultClass.cast(((BooleanQuery) preparedQuery).evaluate());
        } else if (queryType == QueryType.TUPLE) {
            return resultClass.cast(((TupleQuery) preparedQuery).evaluate());
        } else if (queryType == QueryType.GRAPH) {
            return resultClass.cast(((GraphQuery) preparedQuery).evaluate());
        } else {
            throw new Error("Unexpected query type: " + queryType);
        }
    }

    protected final Query prepareQuery(final QuerySpec<?> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final int timeout) throws RepositoryException
    {
        final Query preparedQuery;
        synchronized (this) {
            try {
                preparedQuery = this.connection.prepareQuery(query.getLanguage(),
                        query.getString(), query.getBaseURI());
            } catch (final MalformedQueryException ex) {
                throw new RepositoryException("Cannot prepare query operation: "
                        + "query string rejected as malformed by repository backend", ex);
            }
        }

        final QueryType<?> expectedType = query.getType();
        final QueryType<?> actualType = QueryType.forQueryClass(preparedQuery.getClass());
        if (actualType != expectedType) {
            throw new Error("Expected " + expectedType + ", got " + actualType);
        }

        if (dataset != null) {
            preparedQuery.setDataset(dataset);
        }

        if (bindings != null) {
            preparedQuery.clearBindings();
            for (final String name : bindings.getBindingNames()) {
                preparedQuery.setBinding(name, bindings.getValue(name));
            }
        }

        if (timeout >= 0) {
            preparedQuery.setMaxExecutionTime(timeout);
        }

        preparedQuery.setIncludeInferred(false);

        return preparedQuery;
    }

    /**
     * {@inheritDoc} Note: it is expected that either no inference is performed in the wrapped
     * repository, or if inference is performed, it does not introduce new contexts.
     */
    @Override
    public synchronized CloseableIteration<? extends Resource, RepositoryException> getContextIDs(
            final InferenceMode mode) throws RepositoryException
    {
        return this.connection.getContextIDs();
    }

    @Override
    public synchronized CloseableIteration<? extends Statement, RepositoryException> getStatements(
            @Nullable final Resource subj, @Nullable final IRI pred, @Nullable final Value obj,
            final InferenceMode mode, final Resource... contexts) throws RepositoryException
    {
        return this.connection.getStatements(subj, pred, obj, false, contexts);
    }

    @Override
    public synchronized boolean hasStatement(@Nullable final Resource subj,
            @Nullable final IRI pred, @Nullable final Value obj, final InferenceMode mode,
            final Resource... contexts) throws RepositoryException
    {
        return this.connection.hasStatement(subj, pred, obj, false, contexts);
    }

    @Override
    public synchronized long size(final InferenceMode mode, final Resource... contexts)
            throws RepositoryException
    {
        return this.connection.size(contexts);
    }

    @Override
    public synchronized void update(final UpdateSpec update, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode)
            throws UpdateExecutionException, RepositoryException
    {

        boolean singleDataset = true;
        Dataset actualDataset = null;

        if (dataset != null) {
            actualDataset = dataset;
            singleDataset = true;

        } else if (update.isParsed()) {
            actualDataset = update.getDatasets().get(0);
            singleDataset = true;
            final int size = update.getDatasets().size();
            for (int i = 1; i < size; ++i) {
                if (update.getDatasets().get(i) != actualDataset) {
                    singleDataset = false;
                    break;
                }
            }
        }

        if (singleDataset) {
            try {
                final Update preparedUpdate = this.connection.prepareUpdate(update.getLanguage(),
                        update.getString(), update.getBaseURI());
                if (actualDataset != null) {
                    preparedUpdate.setDataset(actualDataset);
                }
                if (bindings != null) {
                    preparedUpdate.clearBindings();
                    for (final String name : bindings.getBindingNames()) {
                        preparedUpdate.setBinding(name, bindings.getValue(name));
                    }
                }
                preparedUpdate.setIncludeInferred(false);
                preparedUpdate.execute();

            } catch (final MalformedQueryException ex) {
                throw new RepositoryException("Cannot prepare update operation: "
                        + "update string as malformed rejected by repository backend", ex);
            }

        } else {
            for (final UpdateSpec atomicUpdate : update.explode()) {
                this.update(atomicUpdate, null, bindings, mode);
            }
        }
    }

    @Override
    public synchronized void add(final Iterable<? extends Statement> statements,
            final Resource... contexts) throws RepositoryException
    {
        // Note: this seems to be the fastest method among RepositoryConnectionBase.add(...).
        this.connection.add(statements, contexts);
    }

    @Override
    public synchronized void remove(final Iterable<? extends Statement> statements,
            final Resource... contexts) throws RepositoryException
    {
        // Note: implementation of called method in RepositoryConnectionBase should
        // use removeWithoutCommit for performance reasons (signal issue?)
        this.connection.remove(statements, contexts);
    }

    @Override
    public synchronized void remove(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final Resource... contexts) throws RepositoryException
    {
        if (subj == null && pred == null && obj == null) {
            this.connection.clear(contexts);
        } else {
            this.connection.remove(subj, pred, obj, contexts);
        }
    }

    @Override
    public synchronized void reset() throws RepositoryException
    {
        this.connection.clear();
    }

    @Override
    protected synchronized void doEnd(final boolean commit) throws RepositoryException
    {
        if (commit) {
            this.connection.commit();
        } else {
            this.connection.rollback();
        }
    }

    @Override
    protected synchronized void doClose() throws RepositoryException
    {
        this.connection.close();
    }

}
