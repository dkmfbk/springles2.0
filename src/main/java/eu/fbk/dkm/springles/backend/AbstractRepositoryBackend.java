package eu.fbk.dkm.springles.backend;

import java.io.File;

import javax.annotation.Nullable;

import com.bigdata.rdf.sail.BigdataSailRepository;
import com.google.common.base.Preconditions;

import org.openrdf.model.ValueFactory;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.dkm.springles.base.Transaction;
import eu.fbk.dkm.springles.base.Transactions;

/**
 * Abstract backend implementation wrapping a <tt>Repository</tt> instance.
 * <p>
 * This class provides a base implementation of the <tt>Backend</tt> interface that wraps an
 * underlying {@link Repository} instance and provides access to its data through
 * {@link RepositoryTransaction}s. Concrete classes must implement method
 * {@link #initializeRepository(File)} that is called at initialization time and returns the
 * repository object being wrapped. Thread safety is realized through mutually exclusive
 * synchronization of methods accessing the wrapped repository. Note that no check is performed to
 * ensure that invocation of API methods conforms to the prescribed lifecycle: if required, such a
 * check can be obtained by wrapping the backend with
 * {@link Backends#debuggingBackend(Backend, Logger)}.
 * </p>
 */
public abstract class AbstractRepositoryBackend implements Backend
{

    /** Shared log object. */
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRepositoryBackend.class);

    /** The wrapped repository object, assigned at initialization time. */
    private Repository repository;

    /** Flag being <tt>true</tt> if a Bigdata repository has been detected. */
    private boolean isBigdata;

    /**
     * Flag being <tt>true</tt> if the wrapped repository is writable, caching property
     * {@link Repository#isWritable()}.
     */
    private boolean writable;

    /**
     * Default constructor.
     */
    protected AbstractRepositoryBackend()
    {
        this.repository = null;
        this.isBigdata = false;
        this.writable = false;
    }

    /**
     * {@inheritDoc} The method acquires the wrapped repository object by calling
     * {@link #initializeRepository(File)}, then initializes the repository and caches its
     * writable and <tt>ValueFactory</tt> properties.
     */
    @Override
    public final synchronized void initialize(@Nullable final File dataDir)
            throws RepositoryException
    {
        this.repository = initializeRepository(dataDir);
        this.repository.initialize();
        this.writable = this.repository.isWritable();

        if (this.repository.getClass().getName().toLowerCase().contains("bigdata")) {
            this.isBigdata = true;
            LOGGER.debug("Bigdata repository detected: read-only connections enabled");
        }
    }

    /**
     * Acquires and initializes the wrapped repository, based on the supplied optional data
     * directory. This method is called by {@link #initialize(File)} and must return a the wrapped
     * repository instance, non-initialized.
     * 
     * @param dataDir
     *            the data directory, possibly <tt>null</tt> for transient backends
     * @return a non-initialized repository instance
     * @throws RepositoryException
     *             on failure
     */
    protected abstract Repository initializeRepository(@Nullable File dataDir)
            throws RepositoryException;

    /**
     * Provides access to the wrapped repository object. This method can be called only after
     * initialization of the backend.
     * 
     * @return the wrapped repository object
     */
    protected final Repository getRepository()
    {
        Preconditions.checkState(this.repository != null);
        return this.repository;
    }

    /**
     * {@inheritDoc} Returns the writable status of the wrapped repository, as returned by
     * {@link Repository#isWritable()}.
     */
    @Override
    public boolean isWritable()
    {
        return this.writable;
    }

    /**
     * {@inheritDoc} Returns the <tt>ValueFactory</tt> associated to the wrapped repository.
     */
    @Override
    public ValueFactory getValueFactory()
    {
        return this.repository.getValueFactory();
    }

    /**
     * {@inheritDoc} Creates a {@link RepositoryTransaction}, backed by a
     * {@link RepositoryConnection} to the wrapped repository. In case a Bigdata repository is
     * wrapped, the method opens a suitable read-only or read/write repository Bigdata connection,
     * based on the supplied <tt>writable</tt> parameter.
     */
    @Override
    public synchronized Transaction newTransaction(final String id, final boolean writable)
            throws RepositoryException
    {
        final RepositoryConnection connection = this.isBigdata ? getBigdataConnection(
                this.repository, writable) : this.repository.getConnection();
        connection.setAutoCommit(false);

        return writable ? new RepositoryTransaction(id, connection) : Transactions
                .unmodifiableTransaction(new RepositoryTransaction(id, connection));
    }

    /**
     * {@inheritDoc} Closes the wrapped repository.
     */
    @Override
    public synchronized void close() throws RepositoryException
    {
        this.repository.shutDown();
    }

    /**
     * Helper method that opens the appropriate Bigdata repository connection based on the
     * specified <tt>writable</tt> parameter.
     * 
     * @param repository
     *            the Bigdata repository
     * @param writable
     *            the desired <tt>writable</tt> status for the connection
     * @return the opened connection, on success
     * @throws RepositoryException
     *             on failure
     */
    private static RepositoryConnection getBigdataConnection(final Repository repository,
            final boolean writable) throws RepositoryException
    {
        final BigdataSailRepository bigdata = (BigdataSailRepository) repository;
        return writable ? bigdata.getReadOnlyConnection() : bigdata.getConnection();
    }

}
