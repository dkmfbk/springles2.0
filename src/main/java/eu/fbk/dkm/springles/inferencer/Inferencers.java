package eu.fbk.dkm.springles.inferencer;

import java.io.IOException;

import javax.annotation.Nullable;

import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.hash.Hasher;

import eu.fbk.dkm.internal.util.Selector;
import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.Factory;
import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.dkm.springles.SPC;
import eu.fbk.dkm.springles.ruleset.Ruleset;
import eu.fbk.dkm.springles.ruleset.Rulesets;
import eu.fbk.dkm.springles.ruleset.RulesetsRDFPRO;

/**
 * Static factory and utility methods operating on <tt>Inferencer</tt>s.
 * 
 * @apiviz.uses eu.fbk.dkm.springles.inferencer.Inferencer - - - <<create>>
 */
public final class Inferencers
{
	private static final Logger LOGGER = LoggerFactory.getLogger(Inferencers.class);

    private Inferencers()
    {
    }

    public static Inferencer newNullInferencer()
    {
        return new AbstractInferencer() {

            @Override
            protected InferenceMode doInitialize(final String inferredContextPrefix,
                    final Hasher hasher) throws Exception
            {
                hasher.putUnencodedChars(getClass().getName());
                return InferenceMode.NONE;
            }

            @Override
            public Session newSession(final String id, final ClosureStatus closureStatus,
                    final Context context) throws RepositoryException
            {
                return new AbstractSession() {};
            }

        };
    }

    public static Inferencer newVoidInferencer()
    {
        return new VoidInferencer();
    }
    public static Inferencer newRDFProInferencer (final eu.fbk.rdfpro.Ruleset rdfpro_ruleset,
            @Nullable final BindingSet rulesetBindings, final int maxConcurrentRules)
    {
        return new RDFProInferencer(rdfpro_ruleset, rulesetBindings, maxConcurrentRules);
    }
    public static Inferencer newNaiveInferencer(final Ruleset ruleset,
            @Nullable final BindingSet rulesetBindings, final int maxConcurrentRules)
    {
        return new NaiveInferencer(ruleset, rulesetBindings, maxConcurrentRules);
    }
    public static Inferencer newTestInferencer(final Ruleset ruleset,
            @Nullable final BindingSet rulesetBindings, final int maxConcurrentRules)
    {
        return new TestInferencer(ruleset, rulesetBindings, maxConcurrentRules);
    }
    public static Inferencer debuggingInferencer(final Inferencer delegate, final Logger logger)
    {
        return new DebuggingInferencer(delegate, logger);
    }

    // XXX: implement ChainedInferencer and add method
    // "Inferencer chainedInferencer(Inferencer... inferencers)"

    /***
     * Fa la distinzione se viene utilizzato RDFPRO come inferencer o altri inferencer; nel caso di RDFPRO 
     * viene passato all'inferencer il percorso del ruleset invece del rulset generato;
     * il bind viene preso da un file bind.ttl.
     * @param graph
     * @param node
     * @return 
     * @throws IOException
     */
    static Factory<Inferencer> getFactory(final Graph graph, final Resource node) throws IOException
    {
    	 final Ruleset ruleset;
    	 final eu.fbk.rdfpro.Ruleset rdfpro_ruleset;
    	 final int maxConcurrentRules;
    	 final MapBindingSet bindings;
    	 final URI rulesetURI;
    	 
        final Selector s = Selector.select(graph, node);
        final URI type = Iterables.getOnlyElement(Iterables.filter(s.getAll(RDF.TYPE, URI.class),
                Predicates.in(ImmutableList.of(SPC.NULL_INFERENCER, SPC.VOID_INFERENCER,
                        SPC.NAIVE_INFERENCER,SPC.TEST_INFERENCER,SPC.RDFPRO_INFERENCER))));
        if (SPC.RDFPRO_INFERENCER.equals(type)){
        	bindings = new MapBindingSet();
        	rulesetURI = s.get(SPC.HAS_RULESET, URI.class, null);
        	
            rdfpro_ruleset = rulesetURI == null ? null : RulesetsRDFPRO.lookup(rulesetURI.toString());
            	
        	String bindingsString = s.get(SPC.HAS_BINDINGS, String.class, null);
	        if (bindingsString != null) {
	        	bindingsString = bindingsString.replaceAll("\\s+", "");
		        URI l = ValueFactoryImpl.getInstance().createURI(bindingsString.split("=")[0]);
		        bindings.addBinding(bindingsString.split("=")[1],l);
	        }
      		     
        	 ruleset= null;
        	 maxConcurrentRules = 0;  
        }
        else if(SPC.NAIVE_INFERENCER.equals(type)){
	        rulesetURI = s.get(SPC.HAS_RULESET, URI.class, null);
	        ruleset = rulesetURI == null ? null : Rulesets.lookup(rulesetURI);
	        maxConcurrentRules = s.get(SPC.HAS_MAX_CONCURRENT_RULES, Integer.class, 0);
	        for(Ruleset r : Rulesets.list()){
	        	LOGGER.info("{}",r.getURL());
	        }
	        bindings = new MapBindingSet();
	        if (ruleset != null) {
	            for (final String parameter : ruleset.getParameters().keySet()) {
	    	        
	                final String capitalized = Character.toUpperCase(parameter.charAt(0))
	                        + parameter.substring(1);
	                for (final String localName : new String[] { parameter, "has" + capitalized,
	                        "is" + capitalized }) {
	                    final URI property = new URIImpl(SPC.NAMESPACE + localName);
	                    final Value value = s.get(property, Value.class, null);
	                    if (value != null) {
	                        bindings.addBinding(parameter, value);
	                    }
	                }
	            }
	            final String bindingsString = s.get(SPC.HAS_BINDINGS, String.class, null);
	            if (bindingsString != null) {
	                for (final String line : Splitter.on('\n').trimResults().omitEmptyStrings()
	                        .split(bindingsString)) {
	                    final int index = line.indexOf('=');
	                    if (index > 0) {
	                        final String parameter = line.substring(0, index).trim();
	                        final String valueAsString = line.substring(index + 1);
	                        final Value value = ruleset.getParameters().get(parameter) //
	                        instanceof URI ? new URIImpl(valueAsString) : new LiteralImpl(
	                                valueAsString);
	                        bindings.addBinding(parameter, value);
	                    }
	                }
	            }
	        }
	        rdfpro_ruleset = null;
        }else{
        	ruleset = null;
        	bindings = null;
        	maxConcurrentRules = 0;
        	rdfpro_ruleset = null;
        }


        return new Factory<Inferencer>() {

            @Override
            public Inferencer create() throws RepositoryException
            {
                if (SPC.NULL_INFERENCER.equals(type)) {
                    return newNullInferencer();
                } else if (SPC.VOID_INFERENCER.equals(type)) {
                    return newVoidInferencer();
                } else if (SPC.NAIVE_INFERENCER.equals(type)) {
                	if(ruleset != null)
                		return newNaiveInferencer(ruleset, bindings, maxConcurrentRules);
                	else
                		return newVoidInferencer();
                } else if (SPC.TEST_INFERENCER.equals(type)) {
                    return newTestInferencer(ruleset, bindings, maxConcurrentRules);
                } else if (SPC.RDFPRO_INFERENCER.equals(type)) {
                	if(rdfpro_ruleset != null)
                		return newRDFProInferencer(rdfpro_ruleset, bindings, maxConcurrentRules);
                	else
                		return newVoidInferencer();
                }else {
                    throw new Error("Unexpected type: " + type);
                }
            }

        };
    }
}
