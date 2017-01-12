package eu.fbk.dkm.springles.ruleset;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

import eu.fbk.rdfpro.Ruleset;

public final class RulesetsRDFPRO
{

    private static final Logger LOGGER = LoggerFactory.getLogger(RulesetsRDFPRO.class);

    private static final Map<String, Ruleset> REGISTERED_RULESETS = Maps.newConcurrentMap();


    public static boolean register(final String path,final Ruleset ruleset)
    {


        final Ruleset oldRuleset = REGISTERED_RULESETS.get(path);
        final boolean equal = oldRuleset != null && oldRuleset.equals(ruleset);

        if (equal) {
            return false;
        }

        REGISTERED_RULESETS.put(path, ruleset);

        if (oldRuleset != null && !equal) {
            LOGGER.warn("Registration of ruleset " + path + " completed");
        }

        return true;
    }

    public static boolean unregister(final String path)
    {
        Preconditions.checkNotNull(path);
        final Ruleset oldRuleset = REGISTERED_RULESETS.remove(path);
        return oldRuleset != null;
    }

    public static Ruleset lookup(final String path)
    {
    //	System.out.println("RulesetLookup path="+path);
    	List<Ruleset>  listrs= RulesetsRDFPRO.list();
    	for (Ruleset ruleset : listrs) {
    //		System.out.println("Ruleset path="+ruleset);
		}
        Preconditions.checkNotNull(path);
        //for(String s : REGISTERED_RULESETS.keySet())
        	//LOGGER.info(s);
        return REGISTERED_RULESETS.get(path);
    }

    public static List<Ruleset> list()
    {
        final List<Ruleset> result = Lists.newArrayList(REGISTERED_RULESETS.values());
        Collections.sort(result, new Comparator<Ruleset>() {

            @Override
            public int compare(final Ruleset first, final Ruleset second)
            {
                return first.hashCode() == second.hashCode() ? 0 : 1;
            }

        });
        return result;
    }

    public static void load(){
        List<URL> metaURLs;
        try {
            metaURLs = Lists.newArrayList(Iterators.forEnumeration(Ruleset.class.getClassLoader()
                    .getResources("META-INF/rdfpro-rulesets")));
            for (URL url : metaURLs) {
            	
			}
           
        } catch (final IOException ex) {
            throw new Error("Unable to retrieve rulesets declarations");
        }

        int counter = 0;
        for (final URL metaURL : metaURLs) {
            List<String> lines;
            try {
                lines = Resources.readLines(metaURL, Charsets.UTF_8);
            
            } catch (final Exception ex) {
                LOGGER.error("Unable to scan rulesets declarations at " + metaURL + " - ignoring");
                continue;
            }

            LOGGER.info("Processing rulesets declarations at " + metaURL);
           
            for (final String line : lines) {
                String path = line.trim();
                if (RDFFormat.forFileName(path) != null) {
                    final int index = Math.max(0, path.lastIndexOf('.'));
                    path = path.substring(0, index).replace('.', '/') + path.substring(index);
                } else {
                    path = path.replace('.', '/');
                }
              
                final URL rulesetURL = Ruleset.class.getClassLoader().getResource(path);
               
                if (rulesetURL != null) {
                    try {
                        final Ruleset ruleset = Ruleset.fromRDF(rulesetURL.toString());
                        register(rulesetURL.toString(),ruleset);
                        ++counter;
                        LOGGER.info("Loaded ruleset from " + rulesetURL);
                      
                    } catch (final Throwable ex) {
                        LOGGER.error("Failed to load ruleset from " + rulesetURL + ": " //
                                + ex.getMessage() + " - ignoring", ex);
                    }
                } else {
                    LOGGER.warn("Unable to locate ruleset at " + rulesetURL + " - ignoring");
                }
            }
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("{} rulesets registered, {} total", counter, RulesetsRDFPRO.list().size());
        }

    }
    static{
    	load();
    }
    private RulesetsRDFPRO()
    {
    }

}
