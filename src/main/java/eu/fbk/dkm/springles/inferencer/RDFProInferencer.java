package eu.fbk.dkm.springles.inferencer;

import java.util.LinkedList;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.hash.Hasher;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.rdfpro.RuleEngine;
import eu.fbk.rdfpro.Ruleset;

// TODO: binding-aware rule activation/deactivation
// TODO: test with scalable repository
// TODO: semi-naive mode

final class RDFProInferencer extends AbstractInferencer
{

    private static final Logger LOGGER = LoggerFactory.getLogger(RDFProInferencer.class);

    private Ruleset ruleset;

    private final BindingSet rulesetBindings;

    public RDFProInferencer(final Ruleset ruleset, @Nullable final BindingSet rulesetBindings)
    {
        this.ruleset = ruleset;
        this.rulesetBindings = rulesetBindings;
    }

    @Override
    protected InferenceMode doInitialize(final String inferredContextPrefix, final Hasher hasher)
            throws Exception
    {
        final String name = this.rulesetBindings.getBindingNames().iterator().next();
        RDFProInferencer.LOGGER.info("RDFProInferencer.doInitialize BIND {} {} ",
                this.rulesetBindings.getBinding(name).getValue().toString().length(),
                this.rulesetBindings.getBinding(name).getName().toString().length());
        this.ruleset = this.ruleset.rewriteVariables(this.rulesetBindings);
        return InferenceMode.FORWARD;
    }

    @Override
    public Session newSession(final String id, final ClosureStatus closureStatus,
            final Context context) throws RepositoryException
    {
        return new RDFProSession(id, context);
    }

    @Override
    public void close() throws RepositoryException
    {
        if (RDFProInferencer.LOGGER.isInfoEnabled()) {
            RDFProInferencer.LOGGER.info("Inference completed");
        }
    }

    protected class RDFProSession extends AbstractSession
    {

        private final String id;

        private final Context context;

        private List<Statement> buffer;

        public RDFProSession(final String id, final Context context)
        {
            this.id = id;
            this.context = context;
            this.buffer = null;
        }

        @Override
        public final void updateClosure(final ClosureStatus closureStatus)
                throws RepositoryException
        {
            RDFProInferencer.LOGGER.info("[{}] RDFProInferencer updateClosure", this.id);
            final RuleEngine engine = RuleEngine.create(RDFProInferencer.this.ruleset);
            this.buffer = new LinkedList<Statement>();
            final CloseableIteration<? extends Statement, RepositoryException> cl = this.context
                    .getStatements(null, null, null, true);
            while (cl.hasNext()) {
                this.buffer.add(cl.next());
            }
            RDFProInferencer.LOGGER.info("[{}] Statement number before closure: {}", this.id,
                    this.buffer.size());
            final List<Statement> buffer2 = new LinkedList<Statement>();
            buffer2.addAll(this.buffer);
            RDFProInferencer.LOGGER.info("[{}] Statement number before closure: {}", this.id,
                    buffer2.size());
            engine.eval(buffer2);
            buffer2.removeAll(this.buffer);
            RDFProInferencer.LOGGER.info("[{}] Statement Inferred number: {}", this.id,
                    buffer2.size());
            this.context.addInferred(buffer2);
        }

    }

}
