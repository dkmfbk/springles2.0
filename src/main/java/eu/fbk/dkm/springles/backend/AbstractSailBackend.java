package eu.fbk.dkm.springles.backend;

import java.io.File;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.dkm.springles.base.QuerySpec;
import eu.fbk.dkm.springles.base.QueryType;
import eu.fbk.dkm.springles.base.Transaction;
import eu.fbk.dkm.springles.base.Transactions;

/**
 * Abstract backend implementation wrapping a <tt>Sail</tt> instance.
 * <p>
 * This class provides a base implementation of the <tt>Backend</tt> interface that wraps an
 * underlying {@link Sail} instance and provides access to its data through
 * {@link SailTransaction}s. Concrete classes must implement method {@link #initializeSail(File)}
 * that is called at initialization time and returns the sail object actually wrapped. Thread
 * safety is realized through mutually exclusive synchronization of methods accessing the wrapped
 * sail. Note that no check is performed to ensure that invocation of API methods conforms to the
 * prescribed lifecycle: if required, such a check can be obtained by wrapping the backend with
 * {@link Backends#debuggingBackend(Backend, Logger)}.
 * </p>
 */
public abstract class AbstractSailBackend implements Backend
{

    /** Shared log object. */
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSailBackend.class);

    /** The wrapped sail object, assigned at initialization time. */
    private Sail sail;

    /**
     * Flag being <tt>true</tt> if the wrapped sail is writable. Note this flag is redundant, but
     * it is kept as accessing property {@link Sail#isWritable()} may throw exceptions.
     */
    private boolean writable;

    /**
     * Default constructor.
     */
    protected AbstractSailBackend()
    {
        this.sail = null;
        this.writable = false;
    }

    /**
     * {@inheritDoc} The method acquires the wrapped sail object by calling
     * {@link #initializeSail(File)}, then initializes the sail and caches its writable and
     * <tt>ValueFactory</tt> properties.
     */
    @Override
    public synchronized void initialize(@Nullable final File dataDir) throws RepositoryException
    {
        try {
            this.sail = this.initializeSail(dataDir);
            this.sail.initialize();
            this.writable = this.sail.isWritable();

            if (AbstractSailBackend.LOGGER.isInfoEnabled()) {
                final SailConnection connection = this.sail.getConnection();
                try {
                    final String queryString = "SELECT ?n ?m WHERE {\n"
                            + "{ SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o } }\n"
                            + "{ SELECT (COUNT(*) AS ?m) WHERE { GRAPH ?g { ?s ?p ?o } } } }";
                    final QuerySpec<TupleQueryResult> query = QuerySpec.from(QueryType.TUPLE,
                            queryString);
                    final CloseableIteration<? extends BindingSet, QueryEvaluationException> iter;
                    iter = connection.evaluate(query.getExpression(), null,
                            EmptyBindingSet.getInstance(), true);
                    final BindingSet bindings = iter.next();
                    iter.close();
                    final int n = ((Literal) bindings.getValue("n")).intValue();
                    final int m = ((Literal) bindings.getValue("m")).intValue();
                    // final int s = (int) connection.size();
                    AbstractSailBackend.LOGGER.info("{} triples in SAIL default context, "
                            + "{} triples total (reported size: {})", n - m, n);
                } catch (final MalformedQueryException ex) {
                    throw new RepositoryException(ex);
                } catch (final QueryEvaluationException ex) {
                    throw new RepositoryException(ex);
                } finally {
                    connection.close();
                }
            }

        } catch (final SailException ex) {
            throw new RepositoryException(ex);
        }
    }

    /**
     * Acquires and initializes the wrapped sail, based on the supplied optional data directory.
     * This method is called by {@link #initialize(File)} and must return the wrapped sail
     * instance, non-initialized.
     *
     * @param dataDir
     *            the data directory, possibly <tt>null</tt> for transient backends
     * @return a non-initialized sail instance
     * @throws SailException
     *             on failure (will be wrapped and propagated by {@link #initialize(File)}
     * @throws RepositoryException
     *             on failure
     */
    protected abstract Sail initializeSail(@Nullable File dataDir)
            throws SailException, RepositoryException;

    /**
     * Provides access to the wrapped sail object. This method can be called only after
     * initialization of the backend.
     *
     * @return the wrapped sail object
     */
    protected final Sail getSail()
    {
        Preconditions.checkNotNull(this.sail);
        return this.sail;
    }

    /**
     * {@inheritDoc} Returns the writable status of the wrapped sail object.
     */
    @Override
    public boolean isWritable()
    {
        return this.writable;
    }

    /**
     * {@inheritDoc} Returns the <tt>ValueFactory</tt> associated to the wrapped sail object.
     */
    @Override
    public ValueFactory getValueFactory()
    {
        return this.sail.getValueFactory();
    }

    /**
     * {@inheritDoc} Creates a {@link SailTransaction}, backed by a {@link SailConnection} to the
     * wrapped sail. In case a Bigdata sail is wrapped, the method opens a suitable read-only or
     * read/write connection to the Bigdata sail, based on the supplied <tt>writable</tt>
     * parameter.
     */
    @Override
    public synchronized Transaction newTransaction(final String id, final boolean writable)
            throws RepositoryException
    {
        try {
            final ValueFactory factory = this.sail.getValueFactory();
            final SailConnection connection = this.sail.getConnection();
            return writable ? new SailTransaction(id, connection, factory)
                    : Transactions
                            .unmodifiableTransaction(new SailTransaction(id, connection, factory));

        } catch (final SailException ex) {
            throw new RepositoryException(ex);
        }
    }

    /**
     * {@inheritDoc} Closes the wrapped sail.
     */
    @Override
    public synchronized void close() throws RepositoryException
    {
        try {
            this.sail.shutDown();
        } catch (final SailException ex) {
            throw new RepositoryException(ex);
        }
    }

}
