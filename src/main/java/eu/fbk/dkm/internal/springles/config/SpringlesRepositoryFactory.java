package eu.fbk.dkm.internal.springles.config;

import java.util.Collections;
import java.util.Queue;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.config.RepositoryFactory;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;

import eu.fbk.dkm.springles.Factory;
import eu.fbk.dkm.springles.SPC;
import eu.fbk.dkm.springles.SpringlesRepository;

/**
 * Sesame repository factory implementation.
 * <p>
 * This class acts as a bridge between the mechanism to create repositories based on an RDF
 * configuration in Sesame and the RDF-based creation mechanism of {@link Factory}. This class is
 * not intended to be used directly by user, but rather just by Sesame.
 * </p>
 * <p>
 * This class is registered in
 * <tt>META-INF/services/org.openrdf.repository.config.RepositoryFactory</tt> and is activated by
 * Sesame each time a repository of type {@link SpringlesRepository#TYPE} must be created. The
 * implementation is such that the RDF configuration is extracted from the supplied configuration
 * data and used to create a {@link SpringlesRepository} by delegating to the {@link Factory}
 * mechanism.
 * </p>
 */
public final class SpringlesRepositoryFactory implements RepositoryFactory
{

    /**
     * {@inheritDoc} Returns {@link SpringlesRepository#TYPE}.
     */
    @Override
    public String getRepositoryType()
    {
        return SpringlesRepository.TYPE;
    }

    /**
     * {@inheritDoc} Returns a configuration object that extract and stores the RDF configuration
     * data specific to the repository.
     */
    @Override
    public RepositoryImplConfig getConfig()
    {
        return new Config();
    }

    /**
     * {@inheritDoc} Delegates to the {@link Factory} creation mechanism.
     */
    @Override
    public Repository getRepository(final RepositoryImplConfig config)
            throws RepositoryConfigException
    {
        Preconditions.checkNotNull(config);

        if (!(config instanceof Config)) {
            throw new RepositoryConfigException("Invalid configuration class: " + //
                    config.getClass());
        }

        final Config castedConfig = (Config) config;

        try {
            return Factory.get(SpringlesRepository.class, castedConfig.graph, castedConfig.node)
                    .create();
        } catch (final RepositoryException ex) {
            throw new RepositoryConfigException(ex); // due to compliance with Sesame interface
        }
    }

    /**
     * Helper class storing configuration data and adhering to the <tt>RepositoryImplConfig</tt>
     * Sesame API.
     * <p>
     * Instances of this class are just wrappers that store the supplied RDF configuration data
     * (the part related to the repository implementation) so that it can be used when delegating
     * creation and validation to the {@link Factory} mechanism.
     * </p>
     */
    private static final class Config implements RepositoryImplConfig
    {

        /** The RDF configuration graph. */
        private final Model graph;

        /** The root configuration node in the graph. */
        private Resource node;

        /**
         * Creates a new instance with an empty configuration graph. The graph will be populated
         * by {@link #parse(Model, Resource)}.
         */
        public Config()
        {
            this.graph = new LinkedHashModel();
            this.node = SimpleValueFactory.getInstance().createBNode();
        }

        /**
         * {@inheritDoc} Returns {@link SpringlesRepository#TYPE}.
         */
        @Override
        public String getType()
        {
            return SpringlesRepository.TYPE;
        }

        /**
         * {@inheritDoc} Checks that the graph associates the root node to the right repository
         * type ({@link SpringlesRepository#TYPE}), then delegates to {@link Factory}.
         */
        @Override
        public void validate() throws RepositoryConfigException
        {
            try {
                final Set<Value> types = this.graph
                        .filter(this.node, RepositoryConfigSchema.REPOSITORYTYPE, null).objects();
                final Value type = types.size() == 1 ? types.iterator().next() : null;
                final String label = type instanceof Literal ? ((Literal) type).getLabel() : null;
                Preconditions.checkArgument(SpringlesRepository.TYPE.equals(label),
                        "must be " + SpringlesRepository.TYPE);
            } catch (final Throwable ex) {
                throw new RepositoryConfigException(
                        "Invalid repository type in configuration graph: " + ex.getMessage(), ex);
            }

            Factory.get(SpringlesRepository.class, this.graph, this.node);
        }

        /**
         * {@inheritDoc} Dumps stored RDF data to the supplied graph.
         */
        @Override
        public Resource export(final Model graph)
        {
            graph.addAll(this.graph);
            return this.node;
        }

        /**
         * {@inheritDoc} Extracts the RDF triples describing the repository implementation and the
         * resources that are transitively linked (as triple objects) to that implementation. This
         * selection of RDF data is mandatory in order to avoid importing statements about other
         * nodes, particularly the Sesame rep:Repository node (which is related to ID and title
         * metadata about the repository and is distinct from the repository implementation node
         * managed in this class). If information about that node is imported, then it will be
         * also exported as part of the execution of {@link #export(Model)}, causing Sesame to
         * store duplicate data in its system repository, which would then become partly unusable.
         * This method also reuses the Sesame repository ID as the ID assigned through property
         * {@link SPC#HAS_ID} to the repository implementation, if missing.
         */
        @Override
        public void parse(final Model graph, final Resource implNode)
        {
            Preconditions.checkNotNull(graph);
            Preconditions.checkNotNull(implNode);

            // IMPORTANT!!! if we just reuse the supplied implNode bnode, even if the resulting
            // config and the exported RDF appear perfectly fine, they do not work in RDF4J. The
            // problem is that files ~/.RDF4J/server/repository/<ID>/config.ttl miss the required
            // association between repository and repository impl node (repository points to an
            // anon. bnode [], another anon. bnode [] is the impl). No clue why this happens.
            // This has something to do with inlining of bnodes in turtle. Using IRIs in place of
            // bnodes does not work. Adding extra triples to avoid inlining of impl bnode does not
            // work too (it introduces a cycle, which is fine but the turtle writer will complain)
            this.node = SimpleValueFactory.getInstance().createBNode();

            // import only the statements describing implNode
            final Set<Resource> visited = Sets.newHashSet(implNode);
            final Queue<Resource> pending = Lists.newLinkedList(Collections.singleton(implNode));
            while (!pending.isEmpty()) {
                final Resource node = pending.poll();
                for (final Statement stmt : graph.filter(node, null, null)) {

                    // extract components
                    Resource s = stmt.getSubject();
                    final IRI p = stmt.getPredicate();
                    Value o = stmt.getObject();

                    // enqueue the inclusion of statements for object resources
                    if (o instanceof Resource) {
                        final Resource object = (Resource) o;
                        if (!visited.contains(object)) {
                            visited.add(object);
                            pending.offer(object);
                        }
                    }

                    // store the statement, rewriting the implNode with a new IRI if needed
                    s = implNode.equals(s) ? this.node : s;
                    o = implNode.equals(o) ? this.node : o;
                    this.graph.add(s, p, o);
                }
            }

            // reuse the repository identifier if a statement <implNode hasID id> is missing
            if (graph.filter(implNode, SPC.HAS_ID, null).isEmpty()) {
                Statement statement = Iterables.getFirst(
                        graph.filter(null, RepositoryConfigSchema.REPOSITORYIMPL, implNode), null);
                if (statement != null) {
                    statement = Iterables.getFirst(graph.filter(statement.getSubject(),
                            RepositoryConfigSchema.REPOSITORYID, null), null);
                    if (statement != null && statement.getObject() instanceof Literal) {
                        this.graph.add(this.node, SPC.HAS_ID, statement.getObject());
                    }
                }
            }
        }

    }

}
