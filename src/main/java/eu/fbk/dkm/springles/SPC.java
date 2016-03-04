package eu.fbk.dkm.springles;

import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;

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
    public static final URI HAS_ID = create("hasID");

    /** Predicate <tt>:hasBufferingEnabled</tt>. */
    public static final URI HAS_BUFFERING_ENABLED = create("hasBufferingEnabled");

    /** Predicate <tt>:hasMaxConcurrentTransactions</tt>. */
    public static final URI HAS_MAX_CONCURRENT_TRANSACTIONS = create(""
            + "hasMaxConcurrentTransactions");

    // Client-specific configuration

    /** Class <tt>:SpringlesClient</tt>. */
    public static final URI SPRINGLES_CLIENT = create("SpringlesClient");

    /** Predicate <tt>:hasURL</tt>. */
    public static final URI HAS_URL = create("hasURL");

    /** Predicate <tt>:hasUsername</tt>. */
    public static final URI HAS_USERNAME = create("hasUsername");

    /** Predicate <tt>:hasPassword</tt>. */
    public static final URI HAS_PASSWORD = create("hasPassword");

    /** Predicate <tt>:hasConnectionTimeout</tt>. */
    public static final URI HAS_CONNECTION_TIMEOUT = create("hasConnectionTimeout");

    // Store specific configuration

    /** Class <tt>:SpringlesStore</tt>. */
    public static final URI SPRINGLES_STORE = create("SpringlesStore");

    /** Predicate <tt>:hasBackend</tt>. */
    public static final URI HAS_BACKEND = create("hasBackend");

    /** Predicate <tt>:hasInferencer</tt>. */
    public static final URI HAS_INFERENCER = create("hasInferencer");

    /** Predicate <tt>:hasNullContextURI</tt>. */
    public static final URI HAS_NULL_CONTEXT_URI = create("hasNullContextURI");

    /** Predicate <tt>:hasInferredContextPrefix</tt>. */
    public static final URI HAS_INFERRED_CONTEXT_PREFIX = create("hasInferredContextPrefix");

    /** Predicate <tt>:hasServerExtensionEnabled</tt>. */
    public static final URI HAS_SERVER_EXTENSION_ENABLED = create("hasServerExtensionEnabled");

    /** Predicate <tt>:hasMaxTransactionExecutionTime</tt>. */
    public static final URI HAS_MAX_TRANSACTION_EXECUTION_TIME = create(""
            + "hasMaxTransactionExecutionTime");

    /** Predicate <tt>:hasMaxTransactionIdleTime</tt>. */
    public static final URI HAS_MAX_TRANSACTION_IDLE_TIME = create("hasMaxTransactionIdleTime");

    /** Predicate <tt>:hasPreInferenceInterceptors</tt>. */
    public static final URI HAS_PRE_INFERENCE_INTERCEPTORS = create("hasPreInferenceInterceptors");

    /** Predicate <tt>:hasPostInferenceInterceptors</tt>. */
    public static final URI HAS_POST_INFERENCE_INTERCEPTORS = create(""
            + "hasPostInferenceInterceptors");

    // Backends

    /** Class <tt>:RepositoryBackend</tt>. */
    public static final URI REPOSITORY_BACKEND = create("RepositoryBackend");

    /** Class <tt>:SailBackend</tt>. */
    public static final URI SAIL_BACKEND = create("SailBackend");

    /** Class <tt>:MemoryStoreBackend</tt>. */
    public static final URI MEMORY_STORE_BACKEND = create("MemoryStoreBackend");

    /** Class <tt>:NativeStoreBackend</tt>. */
    public static final URI NATIVE_STORE_BACKEND = create("NativeStoreBackend");

    /** Class <tt>:OwlimLiteBackend</tt>. */
    public static final URI OWLIM_LITE_BACKEND = create("OwlimLiteBackend");

    /** Class <tt>:BigdataBackend</tt>. */
    public static final URI BIGDATA_BACKEND = create("BigdataBackend");

    /**
     * Complex property <tt>:wraps</tt> (for {@link #REPOSITORY_BACKEND}, {@link #SAIL_BACKEND};
     * accepts a RDF sail or repository implementation configuration).
     */
    public static final URI WRAPS = create("wraps");

    /**
     * Boolean property <tt>:isPersistent</tt> (for {@link #MEMORY_STORE_BACKEND},
     * {@link #NATIVE_STORE_BACKEND}, {@link #OWLIM_LITE_BACKEND}, {@link #BIGDATA_BACKEND}).
     */
    public static final URI IS_PERSISTENT = create("isPersistent");

    /** Boolean property <tt>:isForceSync</tt> (for {@link #NATIVE_STORE_BACKEND}). */
    public static final URI IS_FORCE_SYNC = create("isForceSync");

    /** String property <tt>:hasTripleIndexes</tt> (for {@link #NATIVE_STORE_BACKEND}). */
    public static final URI HAS_TRIPLE_INDEXES = create("hasTripleIndexes");

    /** Long property <tt>:hasSyncDelay</tt> (for {@link #MEMORY_STORE_BACKEND}). */
    public static final URI HAS_SYNC_DELAY = create("hasSyncDelay");

    /**
     * String property <tt>:hasAdditionalProperties</tt> (for {@link #NATIVE_STORE_BACKEND},
     * {@link #OWLIM_LITE_BACKEND} and {@link #BIGDATA_BACKEND}).
     */
    public static final URI HAS_ADDITIONAL_PROPERTIES = create("hasAdditionalProperties");

    // Null inferencer

    /** Class <tt>:NullInferencer</tt>. */
    public static final URI NULL_INFERENCER = create("NullInferencer");

    // Void inferencer

    /** Class <tt>:VoidInferencer</tt>. */
    public static final URI VOID_INFERENCER = create("VoidInferencer");

    // Naive inferencer

    /** Class <tt>:NaiveRuleInferencer</tt>. */
    public static final URI NAIVE_INFERENCER = create("NaiveInferencer");

    /** Class <tt>:TestInferencer</tt>. */
    public static final URI TEST_INFERENCER = create("TestInferencer");
    
    /** Class <tt>:TestInferencer</tt>. */
    public static final URI RDFPRO_INFERENCER = create("RDFProInferencer");
    
    /** Object property <tt>:hasRuleset</tt> (for {@link #NAIVE_INFERENCER}). */
    public static final URI HAS_RULESET = create("hasRuleset");

    /** Integer property <tt>:hasMaxConcurrentRules</tt> (for {@link #NAIVE_INFERENCER}). */
    public static final URI HAS_MAX_CONCURRENT_RULES = create("hasMaxConcurrentRules");

    /** String property <tt>:hasBindings</tt> (for {@link #NAIVE_INFERENCER}). */
    public static final URI HAS_BINDINGS = create("hasBindings");

    // Rulesets

    /** Individual <tt>:rdfs-merged</tt>. */
    public static final URI RDFS_MERGED = create("rdfs-merged");

    /** Individual <tt>:rdfs-global-import</tt>. */
    public static final URI RDFS_GLOBAL_IMPORT = create("rdfs-global-import");

    /** Individual <tt>:rdfs-graph-import</tt>. */
    public static final URI RDFS_GRAPH_IMPORT = create("rdfs-graph-import");

    /** Individual <tt>:owl2rl-merged</tt>. */
    public static final URI OWL2RL_MERGED = create("owl2rl-merged");

    // Utilities and constructor

    private static URI create(final String localName)
    {
        return ValueFactoryImpl.getInstance().createURI(NAMESPACE, localName);
    }

    private SPC()
    {
    }

}
