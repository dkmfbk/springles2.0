package eu.fbk.dkm.springles.inferencer;

import com.google.common.collect.ForwardingObject;

import org.openrdf.repository.RepositoryException;

import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;

public abstract class ForwardingInferencer extends ForwardingObject implements Inferencer
{

    @Override
    protected abstract Inferencer delegate();

    @Override
    public void initialize(final String inferredContextPrefix) throws RepositoryException
    {
        delegate().initialize(inferredContextPrefix);
    }

    @Override
    public InferenceMode getInferenceMode()
    {
        return delegate().getInferenceMode();
    }

    @Override
    public String getConfigurationDigest()
    {
        return delegate().getConfigurationDigest();
    }

    @Override
    public Session newSession(final String id, final ClosureStatus closureStatus,
            final Context context) throws RepositoryException
    {
        return delegate().newSession(id, closureStatus, context);
    }

    @Override
    public void close() throws RepositoryException
    {
        delegate().close();
    }

}
