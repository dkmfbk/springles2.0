package eu.fbk.dkm.springles;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.util.GraphUtil;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.config.RepositoryConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

/**
 * General-purpose factory based on RDF object specification.
 * <p>
 * This class realizes a general-purpose factory functionality that permits to create objects
 * specified through an RDF configuration and based on a registry of registered providers.
 * </p>
 * <p>
 * The creation mechanism is articulated in two steps:
 * <ol>
 * <li>A factory for a specific object is obtained through method
 * {@link #get(Class, Graph, Resource)}, by specifying the (base) type of object to create and
 * supplying an RDF graph containing an RDF specification of the properties of the object plus the
 * configuration root node in the graph. A registry of providers is consulted in order to create
 * the factory (see below). Errors in the RDF configuration are reported through
 * {@link RepositoryConfigException}s</li>
 * <li>The factory is used to instantiate one (or more) object(s) based on the RDF configuration
 * supplied when creating the factory; errors preventing the creation of the object (not due to
 * bad configurations) are reported through {@link RepositoryException}s.</li>
 * </ol>
 * The two steps reflect the fact that object creation based on some configuration is performed by
 * first parsing and checking the configuration, which is done during the first step of creating
 * the factory, and then performing the actual instantiation, which occurs in the second step.
 * Each step is correlated to its specific exceptions. If only configuration parsing and checking
 * is required, the first step can be performed alone (just get the factory, do not invoke
 * <tt>create</tt>).
 * </p>
 * <p>
 * In order for objects of a certain class to be instantiable through this mechanism, it is
 * necessary to register an implementation <i>provider</i> for that type of object. A provider is
 * identified by a URI, the type of objects produced and a static method (on some class, does not
 * matter which) that can be invoked in order to create a factory based on the RDF configuration
 * at step 1. The exact signature of the provider method is the following:
 * </p>
 * <p style="text-align: center">
 * <tt>T name (Graph graph, Resource node)</tt>
 * </p>
 * <p>
 * A provider is selected as part of the execution of {@link #get(Class, Graph, Resource)}, by
 * matching the RDF type associated to the root nodes with the provider URIs; the factory-creation
 * method of the chosen provider is then invoked to create the factory, by supplying exactly the
 * same RDF graph and root node specified by the caller. Providers must be declared in classpath
 * resources named <tt>META-INF/springles-factory</tt>. Each of these resources is a file
 * consisting in a list of provider declarations separated by <tt>;</tt>, each adhering to the
 * format <tt>uri type method</tt>, where type is a full class name and method is a
 * <tt>class_full_name.method_name</tt> name of the static factory-creation method. An example of
 * declaration is the following:
 * </p>
 * <p style="text-align: center">
 * <tt>http://dkm.fbk.eu/springles#SpringlesStore
 *   eu.fbk.dkm.springles.store.SpringlesStore
 *   eu.fbk.dkm.springles.store.SpringlesStore.getFactory;</tt>
 * </p>
 * 
 * @param <T>
 *            the type of objects produced by the factory
 * @apiviz.stereotype static
 * @apiviz.uses eu.fbk.dkm.springles.SpringlesRepository - - <<create>>
 */
public abstract class Factory<T>
{

    /** Shared log object. */
    private static final Logger LOGGER = LoggerFactory.getLogger(Factory.class);

    /** A map of registered providers, indexed by their URIs. */
    private static final Map<URI, Provider<?>> PROVIDERS = Provider.scan();

    // PUBLIC API

    /**
     * Lists all the provider URIs whose produced objects are compatible with the specified type.
     * 
     * @param type
     *            the produced object (base) type
     * @return a list of provider URIs
     */
    public static List<URI> list(final Class<?> type)
    {
        Preconditions.checkNotNull(type);

        final List<URI> uris = Lists.newArrayList();
        for (final Provider<?> implementation : Factory.PROVIDERS.values()) {
            if (type.isAssignableFrom(implementation.type)) {
                uris.add(implementation.uri);
            }
        }
        return uris;
    }

    /**
     * Creates a factory that produces objects whose type and properties are specified by the
     * supplied RDF data. The method looks at the RDF types associated to the configuration root
     * node and searches a provider whose URI matches one of those type URIs. This provider is
     * then asked to instantiate a factory specific for the configuration data supplied. This
     * process fails with a {@link RepositoryConfigException} in case a unique provider cannot be
     * found or if the provider fails in creating the factory due to bad configuration data.
     * 
     * @param type
     *            the base type of the object to produce
     * @param graph
     *            an RDF graph containing the configuration data
     * @param node
     *            the root configuration node in the RDF graph
     * @param <T>
     *            the type of objects produced by the factory
     * @return a factory able to produce objects corresponding to the supplied configuration data
     * @throws RepositoryConfigException
     *             in case there are no providers matching the configuration supplied or the
     *             configuration is not valid
     */
    public static <T> Factory<T> get(final Class<T> type, final Graph graph, final Resource node)
            throws RepositoryConfigException
    {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(graph);
        Preconditions.checkNotNull(node);

        Provider<T> chosenImplementation = null;

        final Set<Value> values = GraphUtil.getObjects(graph, node, RDF.TYPE);

        for (final Value value : values) {

            if (value instanceof Literal) {
                throw new IllegalArgumentException("Invalid configuration: "
                        + "implementation type not supplied as class URI");
            }

            final Provider<?> implementation = Factory.PROVIDERS.get(value);

            if (implementation == null) {
                continue;

            } else if (!type.isAssignableFrom(implementation.type)) {
                throw new IllegalArgumentException("Implementation URI " + value
                        + " corresponds to factory for type " + implementation.type.getName()
                        + " not compatible with requested type " + type);

            } else if (chosenImplementation == null) {
                @SuppressWarnings("unchecked")
                final Provider<T> castedImplementation = (Provider<T>) implementation;
                chosenImplementation = castedImplementation;

            } else if (chosenImplementation != implementation) {
                throw new IllegalArgumentException(
                        "Multiple implementations match supplied configuration: "
                                + chosenImplementation.type.getName() + ", "
                                + implementation.type.getName());
            }
        }

        if (chosenImplementation == null) {
            throw new UnsupportedOperationException("No factory for node " + node + ", types "
                    + values);
        }

        return chosenImplementation.get(graph, node);
    }

    /**
     * Helper method that creates a factory for a set of objects, each one with its own
     * configuration. The method takes a RDF graph and a set of configuration nodes in that graph.
     * The method creates a factory (using {@link #get(Class, Graph, Resource)}) for each
     * configuration node, and pack them in a new factory object able to create a set of objects
     * by invoking in turns those factories.
     * 
     * @param type
     *            the type of object to be produced and packed in the set
     * @param graph
     *            the RDF configuration graph
     * @param nodes
     *            a set of configuration nodes for the objects to produce, belonging to the same
     *            RDF graph
     * @param <T>
     *            the type of created object
     * @return a factory able to create a set of objects
     * @throws RepositoryConfigException
     *             in case any of the factories wrapped by the returned factory cannot be created
     *             due to bad configuration data or missing provider
     */
    public static <T> Factory<Set<T>> get(final Class<T> type, final Graph graph,
            final Set<? extends Resource> nodes) throws RepositoryConfigException
    {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(graph);

        final List<Factory<T>> factories = Lists.newArrayList();
        for (final Resource node : nodes) {
            factories.add(Factory.get(type, graph, node));
        }

        return new Factory<Set<T>>() {

            @Override
            public Set<T> create() throws RepositoryException
            {
                final ImmutableSet.Builder<T> builder = ImmutableSet.builder();
                for (final Factory<T> factory : factories) {
                    builder.add(factory.create());
                }
                return builder.build();
            }

        };
    }

    /**
     * Helper method that creates a factory for a list of objects, each one with its own
     * configuration. The method takes a RDF graph and a list of configuration nodes in that
     * graph, with possible repetitions. The method creates a factory (using
     * {@link #get(Class, Graph, Resource)}) for each distinct configuration node, and pack them
     * in a new factory object able to create a list of objects by invoking in turns those
     * factories. Note that a unique object will be created for each configuration node, no matter
     * how many times it appears in the supplied configuration node list (i.e., supplying twice
     * the same node leads to creating a list that contains twice a uniquely created object).
     * 
     * @param type
     *            the type of object to be produced and packed in the list
     * @param graph
     *            the RDF configuration graph
     * @param nodes
     *            a list of the configuration nodes for the objects to produce, belonging to the
     *            same RDF graph and with possible repetitions
     * @param <T>
     *            the type of created object
     * @return a factory able to create a list of objects
     * @throws RepositoryConfigException
     *             in case any of the factories wrapped by the returned factory cannot be created
     *             due to bad configuration data or missing provider
     */
    public static <T> Factory<List<T>> get(final Class<T> type, final Graph graph,
            final List<? extends Resource> nodes) throws RepositoryConfigException
    {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(graph);

        final Map<Resource, Factory<T>> map = Maps.newHashMap();
        final List<Factory<T>> factories = Lists.newArrayList();
        for (final Resource node : nodes) {
            Factory<T> factory = map.get(node);
            if (factory == null) {
                factory = Factory.get(type, graph, node);
                map.put(node, factory);
            }
            factories.add(factory);
        }

        return new Factory<List<T>>() {

            @Override
            public List<T> create() throws RepositoryException
            {
                final Map<Factory<T>, T> map = Maps.newIdentityHashMap();
                final ImmutableList.Builder<T> builder = ImmutableList.builder();
                for (final Factory<T> factory : factories) {
                    T object = map.get(factory);
                    if (object == null) {
                        object = factory.create();
                        map.put(factory, object);
                    }
                    builder.add(object);
                }
                return builder.build();
            }

        };
    }

    /**
     * Creates an object based on the configuration data encapsulated in this factory object. Once
     * a factory is obtained, this method can be called multiple times to create objects whose
     * type and properties match the configuration data supplied when creating the factory. The
     * method may fail with a {@link RepositoryException} if an error occurs when creating the
     * object, but it will not fail due to bad configuration data, as this is checked when
     * creating the factory (see discussion on two step process in class overview).
     * 
     * @return the created instance
     * @throws RepositoryException
     *             on failure
     */
    public abstract T create() throws RepositoryException;

    /**
     * A provider entry in the registry of registered providers.
     * 
     * <p>
     * This helper class contains the metadata associated to a provider, namely its URI, the type
     * of produced object and the reflection object for the factory-creation method.
     * </p>
     * 
     * @param <T>
     *            the type of objects produced by the provider.
     */
    private static final class Provider<T>
    {

        /** The name of the resource where to look at for loading providers. */
        private static final String RESOURCE_NAME = "META-INF/springles-factory";

        /** The provider URI. */
        private final URI uri;

        /** The class object for instances produced by provider factories. */
        private final Class<T> type;

        /** The static provider method to invoke to produce a <tt>Factory</tt>. */
        private final Method method;

        /**
         * Creates a new provider instance for the parameters supplied.
         * 
         * @param uri
         *            the provider URI
         * @param type
         *            the type of objects produced by factories returned by the provider.
         * @param method
         *            the static method to invoke to create a factory.
         */
        public Provider(final URI uri, final Class<T> type, final Method method)
        {
            this.uri = uri;
            this.type = type;
            this.method = method;
        }

        /**
         * Creates a factory object configured with the RDF data supplied. The static
         * factory-creation method of the provider is called to instantiate the factory.
         * 
         * @param graph
         *            an RDF graph containing the configuration data for the factory
         * @param node
         *            the root configuration node in the graph
         * @return the requested factory
         * @throws RepositoryConfigException
         *             in case of configuration errors
         * @throws Error
         *             in case the provider factory method cannot be called, for any reason
         */
        public Factory<T> get(final Graph graph, final Resource node)
                throws RepositoryConfigException
        {
            try {
                @SuppressWarnings("unchecked")
                final Factory<T> factory = (Factory<T>) this.method.invoke(null, graph, node);
                return factory;

            } catch (final IllegalAccessException ex) {
                throw new Error("Cannot invoke " + this.method.getName(), ex);

            } catch (final InvocationTargetException ex) {
                final Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                if (cause instanceof RepositoryConfigException) {
                    throw (RepositoryConfigException) cause;
                } else {
                    throw new RepositoryConfigException("Creation failed: "
                            + ex.getCause().getMessage(), ex.getCause());
                }
            }
        }

        /**
         * Returns a one-line representation of the provider object.
         */
        @Override
        public String toString()
        {
            return this.uri + " (type: " + this.type.getName() + ", method: "
                    + this.method.getName() + ")";
        }

        /**
         * Scans the classpath for provider declarations, returning a map of instantiated provider
         * objects.
         * 
         * @return a map containing the instantiated provider objects, indexed by their URI.
         */
        public static Map<URI, Provider<?>> scan()
        {
            final Map<URI, Provider<?>> map = Maps.newHashMap();

            try {
                final Enumeration<URL> e = Factory.class.getClassLoader().getResources(
                        Provider.RESOURCE_NAME);
                while (e.hasMoreElements()) {
                    final URL url = e.nextElement();
                    Factory.LOGGER.debug("Scanning resource {}", url);
                    try {
                        final String declarations = Resources.toString(url, Charsets.UTF_8);
                        for (final String declaration : Splitter.on(';').omitEmptyStrings()
                                .trimResults().split(declarations)) {

                            try {
                                final List<String> tokens = ImmutableList.copyOf(Splitter
                                        .on(CharMatcher.WHITESPACE).trimResults()
                                        .omitEmptyStrings().split(declaration));
                                Preconditions.checkArgument(tokens.size() == 3, "Syntax error");

                                final URI uri = new URIImpl(tokens.get(0));
                                final Class<?> type = Class.forName(tokens.get(1));
                                final Method method = Provider.locate(tokens.get(2));

                                @SuppressWarnings({ "unchecked", "rawtypes" })
                                final Provider<?> provider = new Provider(uri, type, method);

                                map.put(provider.uri, provider);
                                Factory.LOGGER.debug("Registered provider {}", provider);

                            } catch (final Throwable ex) {
                                Factory.LOGGER.error("Error processing declaration in resource "
                                        + url + " (skipping): " + ex.getMessage()
                                        + " - declaration is:\n" + declaration, ex);
                            }

                        }
                    } catch (final Throwable ex) {
                        Factory.LOGGER.error("Error processing resource " + url + //
                                " (skipping): " + ex.getMessage(), ex);
                    }
                }
            } catch (final Throwable ex) {
                Factory.LOGGER.error("Error detecting implementations. Detection halted", ex);
            }

            Factory.LOGGER.debug("{} providers registered", map.size());
            return map;
        }

        /**
         * Helper method that locates the provider static factory-creation method based on its
         * name in a provider declaration.
         * 
         * @param name
         *            the method name in the provider declaration
         * @return the located method, on success
         * @throws ClassNotFoundException
         *             if the name refers to a missing class
         */
        private static Method locate(final String name) throws ClassNotFoundException
        {
            final int index = name.lastIndexOf('.');
            final String className = name.substring(0, index);
            final String methodName = name.substring(index + 1);

            final Class<?> implementationClass = Class.forName(className);
            for (final Method method : implementationClass.getDeclaredMethods()) {
                final Class<?>[] parameters = method.getParameterTypes();
                if (Modifier.isStatic(method.getModifiers())
                        && methodName.equals(method.getName()) && parameters.length == 2
                        && parameters[0] == Graph.class && parameters[1] == Resource.class) {
                    method.setAccessible(true);
                    return method;
                }
            }

            throw new IllegalArgumentException("No suitable method with name '" + methodName
                    + "' in class '" + className + "'");
        }

    }

}
