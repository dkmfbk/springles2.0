package eu.fbk.dkm.springles.backend;

import java.io.File;

import javax.annotation.Nullable;

import com.google.common.collect.ForwardingObject;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.RepositoryException;

import eu.fbk.dkm.springles.base.Transaction;

/**
 * Implementation of <tt>Backend</tt> that forwards by default all method calls to a delegate
 * <tt>Backend</tt>.
 * <p>
 * This class is modeled after the 'forwarding' pattern of Guava (see {@link ForwardingObject}).
 * Subclasses must implement method {@link #delegate()} which is called by other methods and must
 * provide the delegate {@link Backend} instance.
 * </p>
 */
public abstract class ForwardingBackend extends ForwardingObject implements Backend
{

    /**
     * {@inheritDoc}
     */
    @Override
    protected abstract Backend delegate();

    /**
     * {@inheritDoc} Delegates to wrapped backend.
     */
    @Override
    public void initialize(@Nullable final File dataDir) throws RepositoryException
    {
        this.delegate().initialize(dataDir);
    }

    /**
     * {@inheritDoc} Delegates to wrapped backend.
     */
    @Override
    public boolean isWritable()
    {
        return this.delegate().isWritable();
    }

    /**
     * {@inheritDoc} Delegates to wrapped backend.
     */
    @Override
    public ValueFactory getValueFactory()
    {
        return this.delegate().getValueFactory();
    }

    /**
     * {@inheritDoc} Delegates to wrapped backend.
     */
    @Override
    public Transaction newTransaction(final String id, final boolean writable)
            throws RepositoryException
    {
        return this.delegate().newTransaction(id, writable);
    }

    /**
     * {@inheritDoc} Delegates to wrapped backend.
     */
    @Override
    public void close() throws RepositoryException
    {
        this.delegate().close();
    }

}
