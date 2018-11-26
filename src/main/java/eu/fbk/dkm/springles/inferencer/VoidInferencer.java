package eu.fbk.dkm.springles.inferencer;

import java.util.Collections;

import com.google.common.hash.Hasher;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;

// TODO: VoidInferencer is a test inferencer to be either removed or completed

final class VoidInferencer extends AbstractInferencer
{

    private static final Logger LOGGER = LoggerFactory.getLogger(VoidInferencer.class);

    private static final IRI TRIPLES = SimpleValueFactory.getInstance()
            .createIRI("http://rdfs.org/ns/void#triples");

    private IRI inferredContextURI;

    @Override
    protected InferenceMode doInitialize(final String inferredContextPrefix, final Hasher hasher)
    {
        final SimpleValueFactory vf = SimpleValueFactory.getInstance();
        this.inferredContextURI = inferredContextPrefix.endsWith("*") ? vf.createIRI(
                inferredContextPrefix.substring(0, inferredContextPrefix.length() - 1) + "#void")
                : vf.createIRI(inferredContextPrefix);
        VoidInferencer.LOGGER.debug("Inferred context URI is {}", this.inferredContextURI);

        hasher.putUnencodedChars(this.getClass().getName())
                .putUnencodedChars(this.inferredContextURI.toString());

        return InferenceMode.FORWARD;
    }

    @Override
    public Session newSession(final String id, final ClosureStatus closureStatus,
            final Context context) throws RepositoryException
    {
        return new AbstractSession() {

            @Override
            public void updateClosure(final ClosureStatus closureStatus) throws RepositoryException
            {
                context.removeInferred((Resource) null, VoidInferencer.TRIPLES, null,
                        VoidInferencer.this.inferredContextURI);

                final long size = context.size(true) + 1;

                final ValueFactory factory = context.getValueFactory();
                final Resource subject = factory.createBNode();
                final Literal object = factory.createLiteral(size);
                final Statement statement = factory.createStatement(subject,
                        VoidInferencer.TRIPLES, object, VoidInferencer.this.inferredContextURI);

                context.addInferred(Collections.singleton(statement));

                VoidInferencer.LOGGER.debug("Added number of triples: {}", size);
            }

        };
    }

}