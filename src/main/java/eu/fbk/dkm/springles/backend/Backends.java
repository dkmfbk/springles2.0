package eu.fbk.dkm.springles.backend;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.Resources;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.DelegatingRepository;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.config.AbstractRepositoryImplConfig;
import org.eclipse.rdf4j.repository.config.DelegatingRepositoryImplConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryFactory;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.eclipse.rdf4j.repository.config.RepositoryRegistry;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.StackableSail;
import org.eclipse.rdf4j.sail.config.DelegatingSailImplConfig;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.eclipse.rdf4j.sail.config.SailConfigUtil;
import org.eclipse.rdf4j.sail.config.SailFactory;
import org.eclipse.rdf4j.sail.config.SailImplConfig;
import org.eclipse.rdf4j.sail.config.SailRegistry;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.eclipse.rdf4j.sail.nativerdf.ValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.dkm.internal.util.Flushing;
import eu.fbk.dkm.internal.util.Rewriter;
import eu.fbk.dkm.internal.util.Selector;
import eu.fbk.dkm.springles.Factory;
import eu.fbk.dkm.springles.SPC;

/**
 * Static factory and utility methods operating on <tt>Backend</tt>s.
 *
 * @apiviz.landmark
 * @apiviz.uses eu.fbk.dkm.springles.backend.Backend - - - <<create>>
 */
public final class Backends
{

    private static final Logger LOGGER = LoggerFactory.getLogger(Backends.class);

    private static final String DEFAULT_TRIPLE_INDEXES = "spoc,posc";

    private static final String OWLIM_NAMESPACE = "http://www.ontotext.com/trree/owlim#";

    private static final int OWLIM_LITERAL_LIMIT = 1024 * 1024 - 1;

    private static final List<String> OWLIM_SYSTEM_PROPERTIES = ImmutableList.<String>builder()
            .add("base-URL").add("contexts-index-size").add("debug.level").add("entity-index-size")
            .add("jobsize").add("noPersist").add("num.threads.run").add("optimize.rules")
            .add("partialRDFS").add("predicate-index-size").add("repository-type").add("ruleset")
            .add("ruleSet").add("storage-folder").build();

    private Backends()
    {
    }

    private static File newTempDir()
    {
        final File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
        return tempDir;
    }

    public static Backend newSailBackend(final Sail sail)
    {
        Preconditions.checkNotNull(sail);

        if (sail.getClass().getName().toLowerCase().contains("bigdata")) {
            Backends.LOGGER.warn("Created sail backend using Bigdata sail: query evaluation "
                    + "(through TupleExpr and UpdateExpr) is not supported by Bigdata sail, "
                    + "use a BigdataSailRepository instead");
        }

        return new AbstractSailBackend() {

            @Override
            protected Sail initializeSail(final File dataDir) throws SailException
            {
                sail.setDataDir(dataDir);
                return sail;
            }

        };
    }

    public static Backend newSailBackend(final SailImplConfig sailConfig)
    {
        Preconditions.checkNotNull(sailConfig);

        return new AbstractSailBackend() {

            @Override
            protected Sail initializeSail(final File dataDir)
                    throws SailException, RepositoryException
            {
                final Sail sail = this.createSail(sailConfig);
                sail.setDataDir(dataDir);
                return sail;
            }

            private Sail createSail(final SailImplConfig config) throws RepositoryException
            {
                final SailRegistry registry = SailRegistry.getInstance();
                final SailFactory factory = registry.get(config.getType()).get();
                if (factory == null) {
                    throw new RepositoryException("Unsupported Sail: " + config.getType());
                }

                final Sail sail;
                try {
                    sail = factory.getSail(config);
                } catch (final SailConfigException ex) {
                    throw new RepositoryException("Cannot create wrapped Sail", ex);
                }

                // Code taken from SailRepositoryFactory.createSailStack()
                if (config instanceof DelegatingSailImplConfig) {
                    final SailImplConfig delegateConfig = ((DelegatingSailImplConfig) config)
                            .getDelegate();

                    final Sail delegate = this.createSail(delegateConfig);

                    try {
                        ((StackableSail) sail).setBaseSail(delegate);
                    } catch (final ClassCastException e) {
                        throw new RepositoryException("Delegate specified for Sail "
                                + "that is not a DelegatingSail: " + delegate.getClass());
                    }
                }

                return sail;
            }
        };
    }

    public static Backend newRepositoryBackend(final Repository repository)
    {
        Preconditions.checkNotNull(repository);

        return new AbstractRepositoryBackend() {

            @Override
            protected Repository initializeRepository(final File dataDir)
                    throws RepositoryException
            {
                repository.setDataDir(dataDir);
                return repository;
            }

        };
    }

    public static Backend newRepositoryBackend(final RepositoryImplConfig repositoryConfig)
    {
        Preconditions.checkNotNull(repositoryConfig);

        return new AbstractRepositoryBackend() {

            @Override
            protected Repository initializeRepository(@Nullable final File dataDir)
                    throws RepositoryException
            {
                final Repository repository = this.createRepository(repositoryConfig);
                repository.setDataDir(dataDir);
                return repository;
            }

            private Repository createRepository(final RepositoryImplConfig config)
                    throws RepositoryException
            {
                final RepositoryRegistry registry = RepositoryRegistry.getInstance();
                final RepositoryFactory factory = registry.get(config.getType()).get();
                if (factory == null) {
                    throw new RepositoryException("Unsupported repository: " + config.getType());
                }

                final Repository repository;
                try {
                    repository = factory.getRepository(config);
                } catch (final RepositoryConfigException ex) {
                    throw new RepositoryException(ex);
                }

                // Code taken from LocalRepositoryManager.createRepositoryStack()
                if (config instanceof DelegatingRepositoryImplConfig) {

                    final Repository delegate;
                    delegate = this.createRepository(
                            ((DelegatingRepositoryImplConfig) config).getDelegate());

                    try {
                        ((DelegatingRepository) repository).setDelegate(delegate);
                    } catch (final ClassCastException ex) {
                        throw new RepositoryException("Delegate specified for repository "
                                + "that is not a DelegatingRepository: " + delegate.getClass());
                    }
                }

                return repository;
            }

        };
    }

    public static Backend newMemoryStoreBackend(final boolean persistent, final long syncDelay)
    {
        Preconditions.checkArgument(syncDelay >= 0L);

        return new AbstractSailBackend() {

            @Override
            protected Sail initializeSail(final File dataDir)
                    throws SailException, RepositoryException
            {
                if (persistent && dataDir == null) {
                    throw new RepositoryException(
                            "Missing data directory for persistent memory store");
                }
                final MemoryStore store = new MemoryStore();
                store.setPersist(persistent);
                store.setSyncDelay(syncDelay);
                store.setDataDir(dataDir);
                return store;
            }

        };
    }

    // TODO: additional properties here?

    public static Backend newNativeStoreBackend(final boolean persistent, final boolean forceSync,
            @Nullable final String tripleIndexes, final Map<?, ?> additionalProperties)
    {
        return new AbstractSailBackend() {

            @Override
            protected Sail initializeSail(final File dataDir) throws SailException
            {
                final File storageFolder = persistent ? dataDir : Backends.newTempDir();

                final NativeStore store = new NativeStore();
                store.setForceSync(forceSync);
                store.setTripleIndexes(
                        MoreObjects.firstNonNull(tripleIndexes, Backends.DEFAULT_TRIPLE_INDEXES));
                store.setDataDir(storageFolder);

                if (additionalProperties != null) {
                    store.setNamespaceCacheSize(this.getProperty(additionalProperties,
                            "namespaceCacheSize", ValueStore.NAMESPACE_CACHE_SIZE));
                    store.setNamespaceIDCacheSize(this.getProperty(additionalProperties,
                            "namespaceIDCacheSize", ValueStore.NAMESPACE_ID_CACHE_SIZE));
                    store.setValueCacheSize(this.getProperty(additionalProperties, //
                            "valueCacheSize", ValueStore.VALUE_CACHE_SIZE));
                    store.setValueIDCacheSize(this.getProperty(additionalProperties,
                            "valueIDCacheSize", ValueStore.VALUE_ID_CACHE_SIZE));
                }

                return store;
            }

            private int getProperty(final Map<?, ?> properties, final String name,
                    final int defaultValue)
            {
                if (properties != null) {
                    final Object value = properties.get(name);
                    if (value != null) {
                        try {
                            return Integer.parseInt(value.toString().trim());
                        } catch (final NumberFormatException ex) {
                            throw new NumberFormatException(
                                    "Invalid value for property " + name + ": " + ex.getMessage());
                        }
                    }
                }
                return defaultValue;
            }

        };
    }

    public static Backend newOwlimLiteBackend(final boolean persistent,
            @Nullable final Map<?, ?> additionalProperties)
    {

        final Rewriter limiter = new Rewriter() {

            @Override
            public Value rewriteValue(final ValueFactory factory, final Value value)
            {
                if (value instanceof Literal) {
                    final Literal literal = (Literal) value;
                    final String label = literal.getLabel();
                    if (label.length() > Backends.OWLIM_LITERAL_LIMIT) {
                        System.err.println("Warning: literal long " + label.length()
                                + " chars trimmed to " + Backends.OWLIM_LITERAL_LIMIT + " chars");
                        final String trimmedLabel = label.substring(0,
                                Backends.OWLIM_LITERAL_LIMIT);
                        return factory.createLiteral(trimmedLabel);
                    }
                }
                return value;
            }

        };

        final SailFactory sailFactory = SailRegistry.getInstance().get("swiftowlim:Sail").get();
        if (sailFactory == null) {
            throw new UnsupportedOperationException("Unable to retrieve OWLIM Sail factory: "
                    + "OWLIM jar possibly not on the classpath.");
        }

        final Map<String, String> properties = Maps.newHashMap();

        if (additionalProperties != null) {
            for (@SuppressWarnings("rawtypes")
            final Map.Entry entry : additionalProperties.entrySet()) {
                final String property = entry.getKey().toString();
                final String value = entry.getValue().toString();
                if (!property.contains("/")) {
                    properties.put(property, value);
                } else {
                    final String ns = property.substring(0, property.lastIndexOf('/'));
                    if (!Backends.OWLIM_NAMESPACE.equals(ns)) {
                        Backends.LOGGER.warn("Unknown OWLIM property {}. Ignoring", property);
                    } else {
                        properties.put(property.substring(ns.length() + 1), value);
                    }
                }
            }
        }

        properties.put("noPersist", "" + !persistent);

        return new AbstractSailBackend() {

            @Override
            protected Sail initializeSail(final File dataDir)
                    throws SailException, RepositoryException
            {
                final File storageFolder = dataDir != null ? dataDir : Backends.newTempDir();
                // properties.put("storage-folder", storageFolder.getAbsolutePath());

                if (!properties.containsKey("ruleset")) {
                    try {
                        final String rulesetContent = Resources.toString(
                                this.getClass().getResource("empty.pie"), Charsets.UTF_8);
                        final File rulesetFile = new File(storageFolder, "empty.pie");
                        Files.createParentDirs(rulesetFile);
                        Files.write(rulesetContent, rulesetFile, Charsets.UTF_8);
                        properties.put("ruleset", rulesetFile.getPath());
                    } catch (final IOException ex) {
                        throw new RepositoryException(
                                "Failed to configure empty OWLIM ruleset: " + ex.getMessage(), ex);
                    }
                }

                for (final String property : Backends.OWLIM_SYSTEM_PROPERTIES) {
                    System.clearProperty(property);
                }

                final ValueFactory factory = SimpleValueFactory.getInstance();
                final Model graph = new LinkedHashModel();
                final Resource node = factory.createBNode("swiftowlim");
                graph.add(node, factory.createIRI("http://www.openrdf.org/config/sail#sailType"),
                        factory.createLiteral("swiftowlim:Sail"));

                for (final Map.Entry<String, String> entry : properties.entrySet()) {
                    System.setProperty(entry.getKey(), entry.getValue());
                    graph.add(node, factory.createIRI(Backends.OWLIM_NAMESPACE, entry.getKey()),
                            factory.createLiteral(entry.getValue()));
                }

                try {
                    Backends.LOGGER
                            .info("Creating OWLIM repository with properties: " + properties);
                    final SailImplConfig config = sailFactory.getConfig();
                    config.parse(graph, node);
                    final Sail sail = sailFactory.getSail(config);
                    sail.setDataDir(storageFolder);
                    return Rewriter.newRewritingSail(null,
                            Flushing.newFlushingSail(Rewriter.newSkolemizingSail(null, sail)),
                            limiter, Rewriter.IDENTITY);

                } catch (final SailConfigException ex) {
                    throw new RepositoryException("Invalid configuration: " + ex.getMessage(), ex);
                }
            }

        };
    }

    public static Backend newTransientBackend()
    {
        Backend result;
        try {
            result = Backends.newOwlimLiteBackend(false, null);
        } catch (final Throwable ex2) {
            result = Backends.newMemoryStoreBackend(false, 0);
        }
        return result;
    }

    public static Backend debuggingBackend(final Backend delegate, final Logger logger)
    {
        return new DebuggingBackend(delegate, logger);
    }

    static Factory<Backend> getFactory(final Model graph, final Resource node)
            throws RepositoryConfigException
    {
        final Selector s = Selector.select(graph, node);

        final List<IRI> types = s.getAll(RDF.TYPE, IRI.class);
        types.retainAll(ImmutableList.of(SPC.REPOSITORY_BACKEND, SPC.SAIL_BACKEND,
                SPC.MEMORY_STORE_BACKEND, SPC.NATIVE_STORE_BACKEND, SPC.OWLIM_LITE_BACKEND,
                SPC.BIGDATA_BACKEND));
        if (types.size() != 1) {
            throw new RepositoryConfigException("Ambiguous backend type, specified " + types);
        }
        final IRI type = types.get(0);

        final boolean persistent = s.get(SPC.IS_PERSISTENT, Boolean.class, Boolean.TRUE);
        final long syncDelay = s.get(SPC.HAS_SYNC_DELAY, Long.class, 0L);
        final boolean forceSync = s.get(SPC.IS_FORCE_SYNC, Boolean.class, true);
        final String tripleIndexes = s.get(SPC.HAS_TRIPLE_INDEXES, String.class, null);
        final Properties properties = new Properties();
        try {
            properties.load(new StringReader(//
                    s.get(SPC.HAS_ADDITIONAL_PROPERTIES, String.class, "")));
        } catch (final IOException ex) {
            throw new Error("Unexpected exception", ex);
        }

        final Object config;
        if (type.equals(SPC.REPOSITORY_BACKEND)) {
            config = AbstractRepositoryImplConfig.create(graph, s.get(SPC.WRAPS, Resource.class));
            ((RepositoryImplConfig) config).validate();
        } else if (type.equals(SPC.SAIL_BACKEND)) {
            try {
                config = SailConfigUtil.parseRepositoryImpl(graph,
                        s.get(SPC.WRAPS, Resource.class));
                ((SailImplConfig) config).validate();
            } catch (final SailConfigException ex) {
                throw new RepositoryConfigException(ex);
            }
        } else {
            config = null;
        }

        return new Factory<Backend>() {

            @Override
            public Backend create() throws RepositoryException
            {
                if (type.equals(SPC.REPOSITORY_BACKEND)) {
                    return Backends.newRepositoryBackend((RepositoryImplConfig) config);
                } else if (type.equals(SPC.SAIL_BACKEND)) {
                    return Backends.newSailBackend((SailImplConfig) config);
                } else if (type.equals(SPC.MEMORY_STORE_BACKEND)) {
                    return Backends.newMemoryStoreBackend(persistent, syncDelay);
                } else if (type.equals(SPC.NATIVE_STORE_BACKEND)) {
                    return Backends.newNativeStoreBackend(persistent, forceSync, tripleIndexes,
                            properties);
                } else if (type.equals(SPC.OWLIM_LITE_BACKEND)) {
                    return Backends.newOwlimLiteBackend(persistent, properties);
                } else {
                    throw new Error("Unexpected type: " + type);
                }
            }

        };
    }

}
