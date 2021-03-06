package eu.fbk.dkm.springles.backend;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryInterruptedException;
import org.openrdf.query.QueryResultUtil;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.algebra.Load;
import org.openrdf.query.algebra.UpdateExpr;
import org.openrdf.query.impl.ListBindingSet;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.UpdateContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.TimeLimitIteration;

import eu.fbk.dkm.internal.util.Iterations;
import eu.fbk.dkm.internal.util.RDFParseOptions;
import eu.fbk.dkm.internal.util.RDFSource;
import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.dkm.springles.base.QuerySpec;
import eu.fbk.dkm.springles.base.QueryType;
import eu.fbk.dkm.springles.base.UpdateSpec;

public class SailTransaction extends AbstractBackendTransaction {

    private static final Logger LOGGER = LoggerFactory.getLogger(SailTransaction.class);

    private static final BindingSet EMPTY_BINDINGS = new ListBindingSet(
            Collections.<String>emptyList(), Collections.<Value>emptyList());

    private final SailConnection connection;

    public SailTransaction(final String id, final SailConnection connection,
            final ValueFactory valueFactory) {
        super(id, valueFactory);
        Preconditions.checkNotNull(connection);
        this.connection = connection;
        try {
			this.connection.begin();
		} catch (SailException e) {
			
			throw new RuntimeException(e);
			
		}
    }

    protected final SailConnection getConnection() {
        return this.connection;
    }

    @Override
    public synchronized String getNamespace(final String prefix) throws RepositoryException {
        try {
            return this.connection.getNamespace(prefix);
        } catch (final SailException ex) {
            throw new RepositoryException(ex.getMessage(), ex);
        }
    }

    @Override
    public synchronized CloseableIteration<? extends Namespace, RepositoryException> getNamespaces()
            throws RepositoryException {
        try {
            return Iterations.asRepositoryIteration(this.connection.getNamespaces());
        } catch (final SailException ex) {
            throw new RepositoryException(ex.getMessage(), ex);
        }
    }

    @Override
    public synchronized void setNamespace(final String prefix, @Nullable final String name)
            throws RepositoryException {
        try {
            if (name != null) {
                this.connection.setNamespace(prefix, name);
            } else {
                this.connection.removeNamespace(prefix);
            }
        } catch (final SailException ex) {
            throw new RepositoryException(ex.getMessage(), ex);
        }
    }

    @Override
    public synchronized void clearNamespaces() throws RepositoryException {
        try {
            this.connection.clearNamespaces();
        } catch (final SailException ex) {
            throw new RepositoryException(ex.getMessage(), ex);
        }
    }

    @Override
    public void query(final QuerySpec<?> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode, final int timeout,
            final Object handler) throws QueryEvaluationException, RepositoryException {
        final Object result = query(query, dataset, bindings, mode, timeout);

        if (result instanceof TupleQueryResult) {
            final TupleQueryResult tupleResult = (TupleQueryResult) result;
            try {
                QueryResultUtil.report(tupleResult, (TupleQueryResultHandler) handler);
            } catch (final TupleQueryResultHandlerException ex) {
                throw new QueryEvaluationException(ex);
            } finally {
                tupleResult.close();
            }

        } else if (result instanceof GraphQueryResult) {
            final GraphQueryResult graphResult = (GraphQueryResult) result;
            try {
                QueryResultUtil.report(graphResult, (RDFHandler) handler);
            } catch (final RDFHandlerException ex) {
                throw new QueryEvaluationException(ex);
            } finally {
                graphResult.close();
            }
        }
    }

    @Override
    public <T> T query(final QuerySpec<T> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode, final int timeout)
            throws QueryEvaluationException, RepositoryException {
        // Issue the query. Synchronization needed to access Sail connection.
        CloseableIteration<? extends BindingSet, QueryEvaluationException> iteration;
        synchronized (this) {
            try {
                iteration = this.connection.evaluate(query.getExpression(),
                        dataset != null ? dataset : query.getDataset(),
                        bindings != null ? bindings : EMPTY_BINDINGS, false);
            } catch (final SailException ex) {
                throw new RepositoryException(ex);
            }
        }

        // Enforce timeout (code taken from SailQuery implementation).
        if (timeout > 0) {
            iteration = new TimeLimitIteration<BindingSet, QueryEvaluationException>(iteration,
                    TimeUnit.SECONDS.toMillis(timeout)) {

                @Override
                protected void throwInterruptedException() throws QueryEvaluationException {
                    throw new QueryInterruptedException("Query evaluation took too long");
                }

            };
        }

        // Adapt the result.
        final Class<T> resultClass = query.getType().getResultClass();
        if (query.getType() == QueryType.BOOLEAN) {
            return resultClass.cast(Iterations.getFirst(iteration, null) != null);

        } else if (query.getType() == QueryType.GRAPH) {
            return resultClass.cast(Iterations.asGraphQueryResult(iteration,
                    query.getNamespaces(), getValueFactory()));
        } else {
            return resultClass.cast(new TupleQueryResultImpl(Lists.newArrayList(query
                    .getExpression().getBindingNames()), iteration));
        }
    }

    @Override
    public synchronized CloseableIteration<? extends Resource, RepositoryException> getContextIDs(
            final InferenceMode mode) throws RepositoryException {
        try {
            return Iterations.asRepositoryIteration(this.connection.getContextIDs());
        } catch (final SailException ex) {
            throw new RepositoryException(ex.getMessage(), ex);
        }
    }

    @Override
    public synchronized CloseableIteration<? extends Statement, RepositoryException> getStatements(
            @Nullable final Resource subj, @Nullable final URI pred, @Nullable final Value obj,
            final InferenceMode mode, final Resource... contexts) throws RepositoryException {
        try {
            return Iterations.asRepositoryIteration(this.connection.getStatements(subj, pred, obj,
                    false, contexts));
        } catch (final SailException ex) {
            throw new RepositoryException(ex.getMessage(), ex);
        }
    }

    @Override
    public synchronized boolean hasStatement(@Nullable final Resource subj,
            @Nullable final URI pred, @Nullable final Value obj, final InferenceMode mode,
            final Resource... contexts) throws RepositoryException {
        return Iterations.getFirst(getStatements(subj, pred, obj, mode, contexts), null) != null;
    }

    @Override
    public synchronized long size(final InferenceMode mode, final Resource... contexts)
            throws RepositoryException {
        try {
            return this.connection.size(contexts);
        } catch (final SailException ex) {
            throw new RepositoryException(ex.getMessage(), ex);
        }
    }

    @Override
    public synchronized void update(final UpdateSpec update, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode)
            throws UpdateExecutionException, RepositoryException {
        final int numCommands = update.getExpressions().size();
        for (int i = 0; i < numCommands; ++i) {
            final UpdateExpr expr = update.getExpressions().get(i);
            final Dataset ds = dataset != null ? dataset : update.getDatasets().get(i);
            if (expr instanceof Load) {
                executeLoad((Load) expr);
            } else {
                try {
                    this.connection.startUpdate(new UpdateContext(expr, ds, bindings, false));
                } catch (final SailException ex) {
                    throw new RepositoryException(ex.getMessage(), ex);
                }
            }
        }
    }

    /**
     * Helper method implementing the SPARQL Update LOAD operation over the wrapped sail
     * connection. In Sesame, LOAD operation is not implemented in sail but at the level of
     * SailRepository, so to exploit the parsing logic implemented in repositories. As Springles
     * do not build on SailRepository, we need to implement LOAD here, exploiting the parsing
     * logic available in RDFSource.
     * 
     * @param load
     *            the LOAD algebra node
     * @throws UpdateExecutionException
     *             on failure
     */
    protected final void executeLoad(final Load load) throws UpdateExecutionException {
        try {
            final SailConnection conn = this.connection;
            final URL url = new URL(load.getSource().getValue().stringValue());
            final Resource graph = load.getGraph() == null ? null : (Resource) load.getGraph()
                    .getValue();

            RDFSource.deserializeFrom(url, new RDFParseOptions(null, null, getValueFactory()))
                    .streamTo(new RDFHandlerBase() {

                        @Override
                        public void handleNamespace(final String prefix, final String uri)
                                throws RDFHandlerException {
                            try {
                                conn.setNamespace(prefix, uri);
                            } catch (final SailException ex) {
                                throw new RDFHandlerException(ex);
                            }
                        }

                        @Override
                        public void handleStatement(final Statement statement)
                                throws RDFHandlerException {
                            final Resource subj = statement.getSubject();
                            final URI pred = statement.getPredicate();
                            final Value obj = statement.getObject();
                            try {
                                if (graph == null) {
                                    conn.addStatement(subj, pred, obj);
                                } else {
                                    conn.addStatement(subj, pred, obj, graph);
                                }
                            } catch (final SailException ex) {
                                throw new RDFHandlerException(ex);
                            }
                        }

                    });

        } catch (final Exception ex) {
            if (load.isSilent()) {
                LOGGER.warn("Update execution (silent mode) failed", ex);
            } else {
                throw new UpdateExecutionException(
                        ex instanceof RDFHandlerException ? ex.getCause() : ex);
            }
        }
    }

    @Override
    public synchronized void add(final Iterable<? extends Statement> statements,
            final Resource... contexts) throws RepositoryException {
        try {
            // No other way than adding the statements one at a time.
            if (contexts.length > 0) {
                for (final Statement statement : statements) {
                    this.connection.addStatement(statement.getSubject(), statement.getPredicate(),
                            statement.getObject(), contexts);
                }
            } else {
                for (final Statement statement : statements) {
                    this.connection.addStatement(statement.getSubject(), statement.getPredicate(),
                            statement.getObject(), statement.getContext());
                }
            }
        } catch (final SailException ex) {
            throw new RepositoryException(ex.getMessage(), ex);
        }
    }

    @Override
    public synchronized void remove(final Iterable<? extends Statement> statements,
            final Resource... contexts) throws RepositoryException {
        try {
            // No other way than removing the statements one at a time.
            if (contexts.length > 0) {
                for (final Statement statement : statements) {
                    this.connection.removeStatements(statement.getSubject(),
                            statement.getPredicate(), statement.getObject(), contexts);
                }
            } else {
                for (final Statement statement : statements) {
                    this.connection.removeStatements(statement.getSubject(),
                            statement.getPredicate(), statement.getObject(),
                            statement.getContext());
                }
            }
        } catch (final SailException ex) {
            throw new RepositoryException(ex.getMessage(), ex);
        }
    }

    @Override
    public synchronized void remove(@Nullable final Resource subject,
            @Nullable final URI predicate, @Nullable final Value object,
            final Resource... contexts) throws RepositoryException {
        try {
            if (subject == null && predicate == null && object == null) {
                this.connection.clear(contexts);
            } else {
                this.connection.removeStatements(subject, predicate, object, contexts);
            }
        } catch (final SailException ex) {
            throw new RepositoryException(ex.getMessage(), ex);
        }
    }

    @Override
    public synchronized void reset() throws RepositoryException {
        try {
            this.connection.clear();
        } catch (final SailException ex) {
            throw new RepositoryException(ex.getMessage(), ex);
        }
    }

    @Override
    protected synchronized void doEnd(final boolean commit) throws SailException {
        if (commit) {
            this.connection.commit();
        } else {
            this.connection.rollback();
        }
    }

    @Override
    protected synchronized void doClose() throws SailException {
        this.connection.close();
    }

}
