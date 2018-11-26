package eu.fbk.dkm.springles;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Springles configuration vocabulary.
 *
 * @apiviz.stereotype static
 */
public final class SPC
{

    // Namespace

    /** Schema namespace <tt>http://dkm.fbk.eu/springles#</tt>. */
    public static final String NAMESPACE = "http://dkm.fbk.eu/springles/config#";

    // Shared repository configuration

    /** Predicate <tt>:hasID</tt>. */
    public static final IRI HAS_ID = SPC.create("hasID");

    /** Predicate <tt>:hasBufferingEnabled</tt>. */
    public static final IRI HAS_BUFFERING_ENABLED = SPC.create("hasBufferingEnabled");

    /** Predicate <tt>:hasMaxConcurrentTransactions</tt>. */
    public static final IRI HAS_MAX_CONCURRENT_TRANSACTIONS = SPC
            .create("" + "hasMaxConcurrentTransactions");

    // Client-specific configuration

    /** Class <tt>:SpringlesClient</tt>. */
    public static final IRI SPRINGLES_CLIENT = SPC.create("SpringlesClient");

    /** Predicate <tt>:hasURL</tt>. */
    public static final IRI HAS_URL = SPC.create("hasURL");

    /** Predicate <tt>:hasUsername</tt>. */
    public static final IRI HAS_USERNAME = SPC.create("hasUsername");

    /** Predicate <tt>:hasPassword</tt>. */
    public static final IRI HAS_PASSWORD = SPC.create("hasPassword");

    /** Predicate <tt>:hasConnectionTimeout</tt>. */
    public static final IRI HAS_CONNECTION_TIMEOUT = SPC.create("hasConnectionTimeout");

    // Store specific configuration

    /** Class <tt>:SpringlesStore</tt>. */
    public static final IRI SPRINGLES_STORE = SPC.create("SpringlesStore");

    /** Predicate <tt>:hasBackend</tt>. */
    public static final IRI HAS_BACKEND = SPC.create("hasBackend");

    /** Predicate <tt>:hasInferencer</tt>. */
    public static final IRI HAS_INFERENCER = SPC.create("hasInferencer");

    /** Predicate <tt>:hasNullContextURI</tt>. */
    public static final IRI HAS_NULL_CONTEXT_URI = SPC.create("hasNullContextURI");

    /** Predicate <tt>:hasInferredContextPrefix</tt>. */
    public static final IRI HAS_INFERRED_CONTEXT_PREFIX = SPC.create("hasInferredContextPrefix");

    /** Predicate <tt>:hasServerExtensionEnabled</tt>. */
    public static final IRI HAS_SERVER_EXTENSION_ENABLED = SPC.create("hasServerExtensionEnabled");

    /** Predicate <tt>:hasMaxTransactionExecutionTime</tt>. */
    public static final IRI HAS_MAX_TRANSACTION_EXECUTION_TIME = SPC
            .create("" + "hasMaxTransactionExecutionTime");

    /** Predicate <tt>:hasMaxTransactionIdleTime</tt>. */
    public static final IRI HAS_MAX_TRANSACTION_IDLE_TIME = SPC
            .create("hasMaxTransactionIdleTime");

    /** Predicate <tt>:hasPreInferenceInterceptors</tt>. */
    public static final IRI HAS_PRE_INFERENCE_INTERCEPTORS = SPC
            .create("hasPreInferenceInterceptors");

    /** Predicate <tt>:hasPostInferenceInterceptors</tt>. */
    public static final IRI HAS_POST_INFERENCE_INTERCEPTORS = SPC
            .create("" + "hasPostInferenceInterceptors");

    // Backends

    /** Class <tt>:RepositoryBackend</tt>. */
    public static final IRI REPOSITORY_BACKEND = SPC.create("RepositoryBackend");

    /** Class <tt>:SailBackend</tt>. */
    public static final IRI SAIL_BACKEND = SPC.create("SailBackend");

    /** Class <tt>:MemoryStoreBackend</tt>. */
    public static final IRI MEMORY_STORE_BACKEND = SPC.create("MemoryStoreBackend");

    /** Class <tt>:NativeStoreBackend</tt>. */
    public static final IRI NATIVE_STORE_BACKEND = SPC.create("NativeStoreBackend");

    /** Class <tt>:OwlimLiteBackend</tt>. */
    public static final IRI OWLIM_LITE_BACKEND = SPC.create("OwlimLiteBackend");

    /** Class <tt>:BigdataBackend</tt>. */
    public static final IRI BIGDATA_BACKEND = SPC.create("BigdataBackend");

    /**
     * Complex property <tt>:wraps</tt> (for {@link #REPOSITORY_BACKEND}, {@link #SAIL_BACKEND};
     * accepts a RDF sail or repository implementation configuration).
     */
    public static final IRI WRAPS = SPC.create("wraps");

    /**
     * Boolean property <tt>:isPersistent</tt> (for {@link #MEMORY_STORE_BACKEND},
     * {@link #NATIVE_STORE_BACKEND}, {@link #OWLIM_LITE_BACKEND}, {@link #BIGDATA_BACKEND}).
     */
    public static final IRI IS_PERSISTENT = SPC.create("isPersistent");

    /** Boolean property <tt>:isForceSync</tt> (for {@link #NATIVE_STORE_BACKEND}). */
    public static final IRI IS_FORCE_SYNC = SPC.create("isForceSync");

    /** String property <tt>:hasTripleIndexes</tt> (for {@link #NATIVE_STORE_BACKEND}). */
    public static final IRI HAS_TRIPLE_INDEXES = SPC.create("hasTripleIndexes");

    /** Long property <tt>:hasSyncDelay</tt> (for {@link #MEMORY_STORE_BACKEND}). */
    public static final IRI HAS_SYNC_DELAY = SPC.create("hasSyncDelay");

    /**
     * String property <tt>:hasAdditionalProperties</tt> (for {@link #NATIVE_STORE_BACKEND},
     * {@link #OWLIM_LITE_BACKEND} and {@link #BIGDATA_BACKEND}).
     */
    public static final IRI HAS_ADDITIONAL_PROPERTIES = SPC.create("hasAdditionalProperties");

    // Null inferencer

    /** Class <tt>:NullInferencer</tt>. */
    public static final IRI NULL_INFERENCER = SPC.create("NullInferencer");

    // Void inferencer

    /** Class <tt>:VoidInferencer</tt>. */
    public static final IRI VOID_INFERENCER = SPC.create("VoidInferencer");

    // Naive inferencer

    /** Class <tt>:NaiveInferencer</tt>. */
    public static final IRI NAIVE_INFERENCER = SPC.create("NaiveInferencer");

    /** Class <tt>:TestInferencer</tt>. */
    public static final IRI TEST_INFERENCER = SPC.create("TestInferencer");

    /** Class <tt>:RDFProInferencer</tt>. */
    public static final IRI RDFPRO_INFERENCER = SPC.create("RDFProInferencer");

    /** Object property <tt>:hasRuleset</tt> (for {@link #NAIVE_INFERENCER}). */
    public static final IRI HAS_RULESET = SPC.create("hasRuleset");

    /** Integer property <tt>:hasMaxConcurrentRules</tt> (for {@link #NAIVE_INFERENCER}). */
    public static final IRI HAS_MAX_CONCURRENT_RULES = SPC.create("hasMaxConcurrentRules");

    /** String property <tt>:hasBindings</tt> (for {@link #NAIVE_INFERENCER}). */
    public static final IRI HAS_BINDINGS = SPC.create("hasBindings");

    // Rulesets

    /** Individual <tt>:rdfs-merged</tt>. */
    public static final IRI RDFS_MERGED = SPC.create("rdfs-merged");

    /** Individual <tt>:rdfs-global-import</tt>. */
    public static final IRI RDFS_GLOBAL_IMPORT = SPC.create("rdfs-global-import");

    /** Individual <tt>:rdfs-graph-import</tt>. */
    public static final IRI RDFS_GRAPH_IMPORT = SPC.create("rdfs-graph-import");

    /** Individual <tt>:owl2rl-merged</tt>. */
    public static final IRI OWL2RL_MERGED = SPC.create("owl2rl-merged");

    // Utilities and constructor

    private static IRI create(final String localName)
    {
        return SimpleValueFactory.getInstance().createIRI(SPC.NAMESPACE, localName);
    }

    private SPC()
    {
    }

}
