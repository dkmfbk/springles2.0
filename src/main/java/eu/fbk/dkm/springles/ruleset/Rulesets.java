package eu.fbk.dkm.springles.ruleset;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.dkm.springles.SPC;

public final class Rulesets
{

    private static final Logger LOGGER = LoggerFactory.getLogger(Rulesets.class);

    private static final Map<Resource, Ruleset> REGISTERED_RULESETS = Maps.newConcurrentMap();

    public static final Ruleset RDFS_MERGED;

    public static final Ruleset RDFS_GLOBAL_IMPORT;

    public static final Ruleset RDFS_GRAPH_IMPORT;

    public static final Ruleset OWL2RL_MERGED;

    public static boolean register(final Ruleset ruleset)
    {
        ruleset.validate();
        ruleset.freeze();

        final Ruleset oldRuleset = Rulesets.REGISTERED_RULESETS.get(ruleset.getID());
        final boolean equal = oldRuleset != null && oldRuleset.equals(ruleset);

        if (equal) {
            return false;
        }

        Rulesets.REGISTERED_RULESETS.put(ruleset.getID(), ruleset);

        if (oldRuleset != null && !equal) {
            Rulesets.LOGGER.warn("Registration of ruleset " + ruleset.getID() + " ("
                    + ruleset.getLabel() + ") overrides old ruleset " + oldRuleset.getID() + " ("
                    + oldRuleset.getLabel() + ")");
        }

        return true;
    }

    public static boolean unregister(final IRI id)
    {
        Preconditions.checkArgument(!SPC.NAMESPACE.equals(id.getNamespace()),
                "Cannot unregister builtin ruleset " + id);
        final Ruleset oldRuleset = Rulesets.REGISTERED_RULESETS.remove(id);
        return oldRuleset != null;
    }

    public static Ruleset lookup(final Resource id)
    {
        Preconditions.checkNotNull(id);
        return Rulesets.REGISTERED_RULESETS.get(id);
    }

    public static List<Ruleset> list()
    {
        final List<Ruleset> result = Lists.newArrayList(Rulesets.REGISTERED_RULESETS.values());
        Collections.sort(result, new Comparator<Ruleset>() {

            @Override
            public int compare(final Ruleset first, final Ruleset second)
            {
                return first.getID().stringValue().compareTo(second.getID().stringValue());
            }

        });
        return result;
    }

    public static void load()
    {
        List<URL> metaURLs;
        try {
            metaURLs = Lists.newArrayList(Iterators.forEnumeration(
                    Ruleset.class.getClassLoader().getResources("META-INF/springles-rulesets")));
        } catch (final IOException ex) {
            throw new Error("Unable to retrieve rulesets declarations");
        }

        int counter = 0;
        for (final URL metaURL : metaURLs) {
            List<String> lines;
            try {
                lines = Resources.readLines(metaURL, Charsets.UTF_8);
            } catch (final Exception ex) {
                Rulesets.LOGGER.error(
                        "Unable to scan rulesets declarations at " + metaURL + " - ignoring");
                continue;
            }

            Rulesets.LOGGER.info("Processing rulesets declarations at " + metaURL);
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
                        final Ruleset ruleset = new Ruleset(rulesetURL);
                        Rulesets.register(ruleset);
                        ++counter;
                        Rulesets.LOGGER
                                .info("Loaded ruleset " + ruleset.getID() + " from " + rulesetURL);
                    } catch (final Throwable ex) {
                        Rulesets.LOGGER.error("Failed to load ruleset from " + rulesetURL + ": " //
                                + ex.getMessage() + " - ignoring", ex);
                    }
                } else {
                    Rulesets.LOGGER
                            .warn("Unable to locate ruleset at " + rulesetURL + " - ignoring");
                }
            }
        }

        if (Rulesets.LOGGER.isInfoEnabled()) {
            Rulesets.LOGGER.info("{} rulesets registered, {} total", counter,
                    Rulesets.list().size());
        }

    }

    static {
        RDFS_MERGED = Rulesets.lookup(SPC.RDFS_MERGED);
        RDFS_GLOBAL_IMPORT = Rulesets.lookup(SPC.RDFS_GLOBAL_IMPORT);
        RDFS_GRAPH_IMPORT = Rulesets.lookup(SPC.RDFS_GRAPH_IMPORT);
        OWL2RL_MERGED = Rulesets.lookup(SPC.OWL2RL_MERGED);
        Rulesets.load();
    }

    private Rulesets()
    {
    }

}
