package eu.fbk.dkm.springles.inferencer;

import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.hash.Hasher;

import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.rdfpro.Rule;
import eu.fbk.rdfpro.RuleEngine;
import eu.fbk.rdfpro.Ruleset;
import info.aduna.iteration.CloseableIteration;

// TODO: binding-aware rule activation/deactivation
// TODO: test with scalable repository
// TODO: semi-naive mode

class RDFProInferencer extends AbstractInferencer
{

    private static final Logger LOGGER = LoggerFactory.getLogger(RDFProInferencer.class);

    private static final int INITIAL_BUFFER_CAPACITY = 1024;

    private Ruleset ruleset;

    private BindingSet rulesetBindings;

    private final int maxConcurrentRules;

    private final Map<Resource, RuleStatistics> statistics;

    public RDFProInferencer(final Ruleset ruleset, @Nullable final BindingSet rulesetBindings,
            final int maxConcurrentRules)  
    {
        
    	URL path_rules = null;
        int concurrencyLevel = maxConcurrentRules;
        if (concurrencyLevel <= 0) {
            concurrencyLevel = Runtime.getRuntime().availableProcessors();
        }
        
        this.ruleset = ruleset;
        this.rulesetBindings = rulesetBindings;
        this.maxConcurrentRules = concurrencyLevel;
        this.statistics = Maps.newHashMap();
        

        for (final Rule rule : ruleset.getRules()) {
            this.statistics.put(rule.getID(), new RuleStatistics(rule.getID()));
        }
    }

    @Override
    protected InferenceMode doInitialize(final String inferredContextPrefix, final Hasher hasher)
            throws Exception
    {
    	String name = rulesetBindings.getBindingNames().iterator().next();
    	LOGGER.info("BIND {} {} ",rulesetBindings.getBinding(name).getValue().toString().length(),rulesetBindings.getBinding(name).getName().toString().length());
        this.ruleset = ruleset.rewriteVariables(rulesetBindings);

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
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Inference completed");
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
        	LOGGER.info("Elaborazione regole");
        	RuleEngine engine = RuleEngine.create(ruleset);
        	buffer  = new LinkedList<Statement>();
        	CloseableIteration<? extends Statement, RepositoryException> cl=  context.getStatements(null, null, null, true);
        	while(cl.hasNext()){
        		//LOGGER.info("ST:" + cl.next().toString());
        		buffer.add(cl.next());
        	}
        	//context.getStatements(null, null, null, true);
        	//st.addTo(buffer);
        	LOGGER.info("Statement number before closure:: "+ buffer.size());
        	List<Statement> buffer2 = new LinkedList<Statement>();
        	buffer2.addAll(buffer);
        	LOGGER.info("Statement number before closure:: "+ buffer2.size());
        	engine.eval(buffer2);
        	buffer2.removeAll(buffer);
        	LOGGER.info("Statement Inferred number:: "+ buffer2.size());
        	
        	context.addInferred(buffer2);

        }

      
    }
   
    private final static class RuleStatistics
    {

        private final Resource ruleID;

        private int activations;

        private long statements;

        private long time;

        public RuleStatistics(final Resource ruleID)
        {
            this.ruleID = ruleID;
            this.activations = 0;
            this.statements = 0L;
            this.time = 0L;
        }

        public void recordActivations(final long statements, final long time)
        {
            ++this.activations;
            this.statements += statements;
            this.time += time;
        }

        @Override
        public String toString()
        {
            final String id = this.ruleID instanceof URI ? ((URI) this.ruleID).getLocalName()
                    : this.ruleID.stringValue();
            return String.format("%-20s %6d activation(s) %8d inf. statement(s) %8d ms total", id,
                    this.activations, this.statements, this.time);
        }

    }

}
