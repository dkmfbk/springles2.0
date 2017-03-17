package eu.fbk.dkm.springles.inferencer;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.openrdf.repository.RepositoryException;

import eu.fbk.dkm.springles.InferenceMode;

public abstract class AbstractInferencer implements Inferencer
{

    private String inferredContextPrefix;

    private InferenceMode inferenceMode;

    private String configurationDigest;

    @Override
    public final void initialize(final String inferredContextPrefix) throws RepositoryException
    {
        try {
            final Hasher hasher = Hashing.md5().newHasher();

            this.inferredContextPrefix = inferredContextPrefix;
            this.inferenceMode = doInitialize(inferredContextPrefix, hasher);
            this.configurationDigest = hasher.hash().toString();

        } catch (final RuntimeException ex) {
            throw ex;
        } catch (final Exception ex) {
            throw new RepositoryException(ex.getMessage(), ex);
        }
    }

    protected abstract InferenceMode doInitialize(String inferredContextPrefix, Hasher hasher)
            throws Exception;

    protected final String getInferredContextPrefix()
    {
        return this.inferredContextPrefix;
    }

    @Override
    public final InferenceMode getInferenceMode()
    {
        return this.inferenceMode;
    }

    @Override
    public final String getConfigurationDigest()
    {
        return this.configurationDigest;
    }

    @Override
    public void close() throws RepositoryException
    {
    }

}
