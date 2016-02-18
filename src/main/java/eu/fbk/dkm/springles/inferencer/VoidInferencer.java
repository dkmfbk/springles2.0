package eu.fbk.dkm.springles.inferencer;

import java.util.Collections;

import com.google.common.hash.Hasher;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.dkm.springles.inferencer.AbstractSession;

// TODO: VoidInferencer is a test inferencer to be either removed or completed

final class VoidInferencer extends AbstractInferencer
{

    private static final Logger LOGGER = LoggerFactory.getLogger(VoidInferencer.class);

    private static final URI TRIPLES = new URIImpl("http://rdfs.org/ns/void#triples");

    private URI inferredContextURI;

    @Override
    protected InferenceMode doInitialize(final String inferredContextPrefix, final Hasher hasher)
    {
        this.inferredContextURI = inferredContextPrefix.endsWith("*") ? new URIImpl(
                inferredContextPrefix.substring(0, inferredContextPrefix.length() - 1) + "#void")
                : new URIImpl(inferredContextPrefix);
        LOGGER.debug("Inferred context URI is {}", this.inferredContextURI);

        hasher.putUnencodedChars(getClass().getName()).putUnencodedChars(this.inferredContextURI.toString());

        return InferenceMode.FORWARD;
    }

    @Override
    public Session newSession(final String id, final ClosureStatus closureStatus,
            final Context context) throws RepositoryException
    {
        return new AbstractSession() {

            @Override
            public void updateClosure(final ClosureStatus closureStatus)
                    throws RepositoryException
            {
                context.removeInferred((Resource) null, TRIPLES, null,
                        VoidInferencer.this.inferredContextURI);

                final long size = context.size(true) + 1;

                final ValueFactory factory = context.getValueFactory();
                final Resource subject = factory.createBNode();
                final Literal object = factory.createLiteral(size);
                final Statement statement = factory.createStatement(subject, TRIPLES, object,
                        VoidInferencer.this.inferredContextURI);

                context.addInferred(Collections.singleton(statement));

                LOGGER.debug("Added number of triples: {}", size);
            }

        };
    }

}