package eu.fbk.dkm.springles.base;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;

import com.google.common.collect.ForwardingObject;

import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.Update;
import org.openrdf.repository.DelegatingRepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.base.RepositoryBase;
import org.openrdf.repository.base.RepositoryConnectionWrapper;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;

import info.aduna.iteration.Iteration;

import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.dkm.springles.SpringlesConnection;
import eu.fbk.dkm.springles.SpringlesRepository;
import eu.fbk.dkm.springles.TransactionMode;

/**
 * Implementation of <tt>SpringlesConnection</tt> that forwards by default all method calls to a
 * delegate <tt>SpringlesConnection</tt>.
 * 
 * <p>
 * This class is modelled after the 'forwarding' pattern of Guava (see {@link ForwardingObject}).
 * Subclasses must implement method {@link #delegate()} which is called by other methods and must
 * provide the delegate {@link SpringlesConnection} instance.
 * </p>
 * <p>
 * Note that this class does not implement the marker Sesame interface
 * {@link DelegatingRepositoryConnection}: it is up to subclasses to decide whether to implement
 * it, thus exposing the fact they are wrappers and providing the capability to change the
 * delegate connection after instantiation. For the same reason this class does not extend
 * {@link RepositoryConnectionWrapper} (also for not inheriting from {@link RepositoryBase}
 * inderectly through <tt>RepositoryConnectionWrapper</tt>).
 * </p>
 */
public abstract class ForwardingSpringlesConnection extends ForwardingObject implements
        SpringlesConnection
{

    /**
     * {@inheritDoc}
     */
    @Override
    protected abstract SpringlesConnection delegate();

    // CONSTANT PROPERTIES

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public SpringlesRepository getRepository()
    {
        return delegate().getRepository();
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public ValueFactory getValueFactory()
    {
        return delegate().getValueFactory();
    }

    // STATUS MANAGEMENT

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public boolean isOpen() throws RepositoryException
    {
        return delegate().isOpen();
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void close() throws RepositoryException
    {
        delegate().close();
    }

    // TRANSACTION HANDLING

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public TransactionMode getTransactionMode() throws RepositoryException
    {
        return delegate().getTransactionMode();
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void setTransactionMode(final TransactionMode mode) throws RepositoryException
    {
        delegate().setTransactionMode(mode);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public boolean isAutoCommit() throws RepositoryException
    {
        return delegate().isAutoCommit();
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void setAutoCommit(final boolean autoCommit) throws RepositoryException
    {
        delegate().setAutoCommit(autoCommit);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void commit() throws RepositoryException
    {
        delegate().commit();
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void rollback() throws RepositoryException
    {
        delegate().rollback();
    }

    // NAMESPACE MANAGEMENT

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public RepositoryResult<Namespace> getNamespaces() throws RepositoryException
    {
        return delegate().getNamespaces();
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public String getNamespace(final String prefix) throws RepositoryException
    {
        return delegate().getNamespace(prefix);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void setNamespace(final String prefix, final String name) throws RepositoryException
    {
        delegate().setNamespace(prefix, name);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void removeNamespace(final String prefix) throws RepositoryException
    {
        delegate().removeNamespace(prefix);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void clearNamespaces() throws RepositoryException
    {
        delegate().clearNamespaces();
    }

    // QUERY HANDLING

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public Query prepareQuery(final QueryLanguage ql, final String query)
            throws RepositoryException, MalformedQueryException
    {
        return delegate().prepareQuery(ql, query);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public Query prepareQuery(final QueryLanguage ql, final String query, final String baseURI)
            throws RepositoryException, MalformedQueryException
    {
        return delegate().prepareQuery(ql, query, baseURI);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public TupleQuery prepareTupleQuery(final QueryLanguage ql, final String query)
            throws RepositoryException, MalformedQueryException
    {
        return delegate().prepareTupleQuery(ql, query);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public TupleQuery prepareTupleQuery(final QueryLanguage ql, final String query,
            final String baseURI) throws RepositoryException, MalformedQueryException
    {
        return delegate().prepareTupleQuery(ql, query, baseURI);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public GraphQuery prepareGraphQuery(final QueryLanguage ql, final String query)
            throws RepositoryException, MalformedQueryException
    {
        return delegate().prepareGraphQuery(ql, query);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public GraphQuery prepareGraphQuery(final QueryLanguage ql, final String query,
            final String baseURI) throws RepositoryException, MalformedQueryException
    {
        return delegate().prepareGraphQuery(ql, query, baseURI);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public BooleanQuery prepareBooleanQuery(final QueryLanguage ql, final String query)
            throws RepositoryException, MalformedQueryException
    {
        return delegate().prepareBooleanQuery(ql, query);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public BooleanQuery prepareBooleanQuery(final QueryLanguage ql, final String query,
            final String baseURI) throws RepositoryException, MalformedQueryException
    {
        return delegate().prepareBooleanQuery(ql, query, baseURI);
    }

    // UPDATE HANDLING

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public Update prepareUpdate(final QueryLanguage ql, final String update)
            throws RepositoryException, MalformedQueryException
    {
        return delegate().prepareUpdate(ql, update);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public Update prepareUpdate(final QueryLanguage ql, final String update, final String baseURI)
            throws RepositoryException, MalformedQueryException
    {
        return delegate().prepareUpdate(ql, update, baseURI);
    }

    // READ METHODS

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public RepositoryResult<Resource> getContextIDs() throws RepositoryException
    {
        return delegate().getContextIDs();
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public RepositoryResult<Resource> getContextIDs(final boolean includeInferred)
            throws RepositoryException
    {
        return delegate().getContextIDs(includeInferred);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public RepositoryResult<Statement> getStatements(final Resource subj, final URI pred,
            final Value obj, final boolean includeInferred, final Resource... contexts)
            throws RepositoryException
    {
        return delegate().getStatements(subj, pred, obj, includeInferred, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public boolean hasStatement(final Resource subj, final URI pred, final Value obj,
            final boolean includeInferred, final Resource... contexts) throws RepositoryException
    {
        return delegate().hasStatement(subj, pred, obj, includeInferred, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public boolean hasStatement(final Statement st, final boolean includeInferred,
            final Resource... contexts) throws RepositoryException
    {
        return delegate().hasStatement(st, includeInferred, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void export(final RDFHandler handler, final Resource... contexts)
            throws RepositoryException, RDFHandlerException
    {
        delegate().export(handler, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void exportStatements(final Resource subj, final URI pred, final Value obj,
            final boolean includeInferred, final RDFHandler handler, final Resource... contexts)
            throws RepositoryException, RDFHandlerException
    {
        delegate().exportStatements(subj, pred, obj, includeInferred, handler, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public long size(final Resource... contexts) throws RepositoryException
    {
        return delegate().size(contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public long size(final boolean includeInferred, final Resource... contexts)
            throws RepositoryException
    {
        return delegate().size(includeInferred, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public boolean isEmpty() throws RepositoryException
    {
        return delegate().isEmpty();
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public boolean isEmpty(final boolean includeInferred) throws RepositoryException
    {
        return delegate().isEmpty(includeInferred);
    }

    // ADD METHODS (FROM STREAMS)

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public ParserConfig getParserConfig()
    {
        return delegate().getParserConfig();
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void setParserConfig(final ParserConfig config)
    {
        delegate().setParserConfig(config);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void add(final InputStream in, final String baseURI, final RDFFormat dataFormat,
            final Resource... contexts) throws IOException, RDFParseException, RepositoryException
    {
        delegate().add(in, baseURI, dataFormat, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void add(final Reader reader, final String baseURI, final RDFFormat dataFormat,
            final Resource... contexts) throws IOException, RDFParseException, RepositoryException
    {
        delegate().add(reader, baseURI, dataFormat, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void add(final URL url, final String baseURI, final RDFFormat dataFormat,
            final Resource... contexts) throws IOException, RDFParseException, RepositoryException
    {
        delegate().add(url, baseURI, dataFormat, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void add(final File file, final String baseURI, final RDFFormat dataFormat,
            final Resource... contexts) throws IOException, RDFParseException, RepositoryException
    {
        delegate().add(file, baseURI, dataFormat, contexts);
    }

    // ADD METHODS (FROM STATEMENTS)

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void add(final Resource subject, final URI predicate, final Value object,
            final Resource... contexts) throws RepositoryException
    {
        delegate().add(subject, predicate, object, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void add(final Statement st, final Resource... contexts) throws RepositoryException
    {
        delegate().add(st, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public <E extends Exception> void add(final Iteration<? extends Statement, E> statements,
            final Resource... contexts) throws RepositoryException, E
    {
        delegate().add(statements, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void add(final Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException
    {
        delegate().add(statements, contexts);
    }

    // REMOVE METHODS

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void remove(final Resource subject, final URI predicate, final Value object,
            final Resource... contexts) throws RepositoryException
    {
        delegate().remove(subject, predicate, object, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void remove(final Statement st, final Resource... contexts) throws RepositoryException
    {
        delegate().remove(st, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public <E extends Exception> void remove(final Iteration<? extends Statement, E> statements,
            final Resource... contexts) throws RepositoryException, E
    {
        delegate().remove(statements, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void remove(final Iterable<? extends Statement> statements, final Resource... contexts)
            throws RepositoryException
    {
        delegate().remove(statements, contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void clear(final Resource... contexts) throws RepositoryException
    {
        delegate().clear(contexts);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void reset() throws RepositoryException
    {
        delegate().reset();
    }

    // INFERENCE MANAGEMENT

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public InferenceMode getInferenceMode() throws RepositoryException
    {
        return delegate().getInferenceMode();
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void setInferenceMode(final InferenceMode mode) throws RepositoryException
    {
        delegate().setInferenceMode(mode);
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public ClosureStatus getClosureStatus() throws RepositoryException
    {
        return delegate().getClosureStatus();
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void updateClosure() throws RepositoryException
    {
        delegate().updateClosure();
    }

    /**
     * {@inheritDoc} Delegates to wrapped connection.
     */
    @Override
    public void clearClosure() throws RepositoryException
    {
        delegate().clearClosure();
    }

}
