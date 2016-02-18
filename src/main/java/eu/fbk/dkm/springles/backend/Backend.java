package eu.fbk.dkm.springles.backend;

import java.io.File;

import javax.annotation.Nullable;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryException;

import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.dkm.springles.base.QueryType;
import eu.fbk.dkm.springles.base.Transaction;

/**
 * Backend abstraction.
 * <p>
 * A backend provides access to the Sesame repository or sail object wrapped by a Springles
 * repository, which provides storage and querying capabilities. The backend abstraction defines
 * an API through which different storage implementations can be exploited in Springles. The
 * lifecycle of a backend is articulated in the following phases:
 * <ul>
 * <li>Instantiation and configuration, through implementation specific constructor and property
 * setter methods.</li>
 * <li>Initialization, through method {@link #initialize(File)} that specifies the data directory
 * where the backend can store its data (this is the same data directory the enclosing Springles
 * repository is initialized with and may be <tt>null</tt> in case of transient repositories).</li>
 * <li>Access to backend properties ({@link #isWritable()} and {@link #getValueFactory()}) and
 * creation of transactions ({@link #newTransaction(String, boolean)}) through which data in the
 * backend can be read, queried or modified. Note that properties can be accessed only after
 * initialization, as their value may depend on the data directory supplied to
 * {@link #initialize(File)}.</li>
 * <li>Disposal, through method {@link #close()} that releases allocated resources. For backend
 * that does not support 'durability' as part of ACID transaction properties (e.g., Sesame memory
 * and native stores under certain configurations), data can be considered safely written to disk
 * only after this method is successfully called.</li>
 * </ul>
 * </p>
 * <p>
 * Static factory methods in {@link Backends} permits to create <tt>Backend</tt> instances for
 * several common use sail and repository implementations, while abstract classes
 * {@link AbstractRepositoryBackend} and {@link AbstractSailBackend} can be extended to ease the
 * implementation of this interface.
 * </p>
 * <p>
 * Implementations of this class must be thread safe, implementing suitable synchronization
 * mechanism if necessary.
 * </p>
 * 
 * @apiviz.landmark
 * @apiviz.uses eu.fbk.dkm.springles.base.Transaction - - - <<create>>
 */
public interface Backend
{

    /**
     * Initializes the backend. This method is called to initialize the backend, supplying the
     * data directory where the backend can store its data.
     * 
     * @param dataDir
     *            an optional data directory where the backend can persist its RDF data
     * @throws RepositoryException
     *             on failure
     */
    void initialize(@Nullable File dataDir) throws RepositoryException;

    /**
     * Specifies whether the backend supports write access (delete / add statements). This method
     * is called only after the backend is initialized, as the writable status may depend on the
     * data directory supplied at initialization time.
     * 
     * @return <tt>true</tt> if the backend supports write access
     */
    boolean isWritable();

    /**
     * Returns the <tt>ValueFactory</tt> associated to the backend. This method is called only
     * after the backend is initialized.
     * 
     * @return the <tt>ValueFactory</tt> associated to the backend
     */
    ValueFactory getValueFactory();

    /**
     * Creates a new transaction manipulating data in the backend. The returned transaction is not
     * expected to support inference ({@link InferenceMode} parameters and closure-related
     * operations are ignored) as well as named queries and update operations (methods
     * {@link Transaction#query(URI, QueryType, InferenceMode, Object...)} and
     * {@link Transaction#update(URI, InferenceMode, Object...)} are unsupported).
     * 
     * @param id
     *            the ID of the created transaction
     * @param writable
     *            <tt>true</tt> if the transaction must support read/write access
     * @return the created transaction
     * @throws RepositoryException
     *             on failure, including the case a read/write transaction is requested but the
     *             backend is read-only
     */
    Transaction newTransaction(String id, boolean writable) throws RepositoryException;

    /**
     * Closes the backend. This method is called to close the backend and release the associated
     * resources. For some <tt>Backend</tt> implementations, properly closing the backend is
     * mandatory in order to avoid data losses (see specific implementation details).
     * 
     * @throws RepositoryException
     *             on failure
     */
    void close() throws RepositoryException;

}
