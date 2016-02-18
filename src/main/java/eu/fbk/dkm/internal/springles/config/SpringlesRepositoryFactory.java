package eu.fbk.dkm.internal.springles.config;

import java.util.Collections;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.util.GraphUtil;
import org.openrdf.model.util.GraphUtilException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.config.RepositoryConfigException;
import org.openrdf.repository.config.RepositoryConfigSchema;
import org.openrdf.repository.config.RepositoryFactory;
import org.openrdf.repository.config.RepositoryImplConfig;

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
        private final Graph graph;

        /** The root configuration node in the graph. */
        private Resource node;

        /**
         * Creates a new instance with an empty configuration graph. The graph will be populated
         * by {@link #parse(Graph, Resource)}.
         */
        public Config()
        {
            this.graph = new GraphImpl();
            this.node = this.graph.getValueFactory().createBNode();
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
                Preconditions.checkArgument(SpringlesRepository.TYPE.equals(GraphUtil
                        .getUniqueObjectLiteral(this.graph, this.node,
                                RepositoryConfigSchema.REPOSITORYTYPE).getLabel()), "must be "
                        + SpringlesRepository.TYPE);
            } catch (final GraphUtilException ex) {
                throw new RepositoryConfigException(
                        "Invalid repository type in configuration graph: " + ex.getMessage(), ex);
            }

            Factory.get(SpringlesRepository.class, this.graph, this.node);
        }

        /**
         * {@inheritDoc} Dumps stored RDF data to the supplied graph.
         */
        @Override
        public Resource export(final Graph graph)
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
         * also exported as part of the execution of {@link #export(Graph)}, causing Sesame to
         * store duplicate data in its system repository, which would then become partly unusable.
         * This method also reuses the Sesame repository ID as the ID assigned through property
         * {@link SPC#HAS_ID} to the repository implementation, if missing.
         */
        @Override
        public void parse(final Graph graph, final Resource implNode)
        {
            Preconditions.checkNotNull(graph);
            Preconditions.checkNotNull(implNode);

            this.node = implNode;

            // import only the statements describing implNode
            final Set<Resource> visited = Sets.newHashSet(implNode);
            final Queue<Resource> pending = Lists.newLinkedList(Collections.singleton(implNode));
            while (!pending.isEmpty()) {
                final Resource node = pending.poll();
                for (final Iterator<Statement> i = graph.match(node, null, null); i.hasNext();) {
                    final Statement statement = i.next();
                    this.graph.add(statement);
                    if (statement.getObject() instanceof Resource) {
                        final Resource object = (Resource) statement.getObject();
                        if (!visited.contains(object)) {
                            visited.add(object);
                            pending.offer(object);
                        }
                    }
                }
            }

            // reuse the repository identifier if a statement <implNode hasID id> is missing
            if (!graph.match(implNode, SPC.HAS_ID, null).hasNext()) {
                Statement statement = Iterators.getNext(
                        graph.match(null, RepositoryConfigSchema.REPOSITORYIMPL, implNode), null);
                if (statement != null) {
                    statement = Iterators.getNext(graph.match(statement.getSubject(),
                            RepositoryConfigSchema.REPOSITORYID, null), null);
                    if (statement != null && statement.getObject() instanceof Literal) {
                        this.graph.add(implNode, SPC.HAS_ID, statement.getObject());
                    }
                }
            }
        }

    }

}
