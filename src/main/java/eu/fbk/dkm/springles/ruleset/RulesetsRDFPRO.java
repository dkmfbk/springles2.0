package eu.fbk.dkm.springles.ruleset;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.Ruleset;

public final class RulesetsRDFPRO
{

    private static final Logger LOGGER = LoggerFactory.getLogger(RulesetsRDFPRO.class);

    private static final Map<String, Ruleset> REGISTERED_RULESETS = Maps.newConcurrentMap();

    public static boolean register(final String path, final Ruleset ruleset)
    {
        final Ruleset oldRuleset = RulesetsRDFPRO.REGISTERED_RULESETS.get(path);

        if (oldRuleset != null && oldRuleset.equals(ruleset)) {
            return false;
        }

        RulesetsRDFPRO.REGISTERED_RULESETS.put(path, ruleset);

        if (oldRuleset != null) {
            RulesetsRDFPRO.LOGGER.warn("Registration of ruleset " + path
                    + " overrides ruleset previously registered");
        }

        return true;
    }

    public static boolean unregister(final String path)
    {
        Preconditions.checkNotNull(path);
        final Ruleset oldRuleset = RulesetsRDFPRO.REGISTERED_RULESETS.remove(path);
        return oldRuleset != null;
    }

    public static Ruleset lookup(final String path)
    {
        Preconditions.checkNotNull(path);
        return RulesetsRDFPRO.REGISTERED_RULESETS.get(path);
    }

    public static List<Ruleset> list()
    {
        return Lists.newArrayList(RulesetsRDFPRO.REGISTERED_RULESETS.values());
    }

    public static void load()
    {
        List<URL> metaURLs;
        try {
            metaURLs = Lists.newArrayList(Iterators.forEnumeration(
                    Ruleset.class.getClassLoader().getResources("META-INF/rdfpro-rulesets")));
        } catch (final IOException ex) {
            throw new Error("Unable to retrieve rulesets declarations");
        }

        int counter = 0;
        for (final URL metaURL : metaURLs) {

            List<String> lines;
            try {
                lines = Resources.readLines(metaURL, Charsets.UTF_8);
            } catch (final Exception ex) {
                RulesetsRDFPRO.LOGGER.error(
                        "Unable to scan rulesets declarations at " + metaURL + " - ignoring");
                continue;
            }

            RulesetsRDFPRO.LOGGER.info("Processing rulesets declarations at " + metaURL);

            for (final String line : lines) {
                String path = line.trim();
                if (Rio.getParserFormatForFileName(path) != null) {
                    final int index = Math.max(0, path.lastIndexOf('.'));
                    path = path.substring(0, index).replace('.', '/') + path.substring(index);
                } else {
                    path = path.replace('.', '/');
                }

                final URL rulesetURL = Ruleset.class.getClassLoader().getResource(path);

                if (rulesetURL != null) {
                    try {
                        final Ruleset ruleset = Ruleset.fromRDF(rulesetURL.toString());
                        RulesetsRDFPRO.register(rulesetURL.toString(), ruleset);
                        ++counter;
                        RulesetsRDFPRO.LOGGER.info("Loaded ruleset from " + rulesetURL);

                    } catch (final Throwable ex) {
                        RulesetsRDFPRO.LOGGER
                                .error("Failed to load ruleset from " + rulesetURL + ": " //
                                        + ex.getMessage() + " - ignoring", ex);
                    }
                } else {
                    RulesetsRDFPRO.LOGGER
                            .warn("Unable to locate ruleset at " + rulesetURL + " - ignoring");
                }
            }
        }

        if (RulesetsRDFPRO.LOGGER.isInfoEnabled()) {
            RulesetsRDFPRO.LOGGER.info("{} rulesets registered, {} total", counter,
                    RulesetsRDFPRO.list().size());
        }
    }

    static {
        RulesetsRDFPRO.load();
    }

    private RulesetsRDFPRO()
    {
        throw new Error();
    }

}
