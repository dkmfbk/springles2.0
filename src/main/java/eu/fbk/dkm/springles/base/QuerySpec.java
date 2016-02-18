package eu.fbk.dkm.springles.base;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParserFactory;
import org.openrdf.query.parser.QueryParserRegistry;
import org.openrdf.query.parser.QueryParserUtil;

import eu.fbk.dkm.internal.util.Algebra;
import eu.fbk.dkm.internal.util.SparqlRenderer;

/**
 * A query specification.
 * <p>
 * This class models the specification of a query (similarly to {@link UpdateSpec} that models an
 * update operation specification), combining in a single object both a language-specific string
 * representation (e.g., SPARQL) and the Sesame algebraic representation:
 * </p>
 * <ul>
 * <li>the string representation is encoded through properties {@link #getString()},
 * {@link #getLanguage()} and {@link #getBaseURI()}, the latter being the optional URI used to
 * interpret relative URIs in the update string;</li>
 * <li>the algebraic representation is encoded through properties {@link #getExpression()},
 * {@link #getDataset()} and {@link #getNamespaces()}; a query has an algebraic tuple expression
 * optionally associated to a dataset (as an effect of <tt>FROM</tt> clauses) and to a set of
 * namespace declarations.</li>
 * </ul>
 * <p>
 * In addition, property {@link #getType()} specifies the type of query, namely a boolean, tuple
 * or graph query.
 * </p>
 * <p>
 * A <tt>QuerySpec</tt> can be created in two ways: either providing its type and string
 * representation (methods {@link #from(QueryType, String)} and
 * {@link #from(QueryType, String, QueryLanguage, String)}); or providing its type and algebraic
 * representation (methods {@link #from(QueryType, TupleExpr, Dataset)} and
 * {@link #from(QueryType, TupleExpr, Dataset, Map)}). In both case, the dual representation is
 * automatically derived, either via parsing or rendering to SPARQL language:
 * </p>
 * <ul>
 * <li>automatic parsing is supported only for {@link QueryLanguage}s for which a parser is
 * available in Sesame: if it is not the case, property {@link #isParsed()} will be set to
 * <tt>false</tt>, the algebraic representation will not be computed and accessing the
 * corresponding three properties will result in an error;</li>
 * <li>automatic rendering of an algebraic expression to a corresponding SPARQL string is always
 * possible; when this happens, property {@link #isRendered()} is set to <tt>true</tt>.</li>
 * </ul>
 * <p>
 * <tt>QuerySpec</tt> instances are created through the <tt>from</tt> factory methods which
 * provide for the caching and reuse of already created objects, thus reducing parsing overhead.
 * Serialization is supported, with deserialization attempting to reuse existing objects from the
 * cache. Instances have to considered to be immutable: while it is not possible to (efficiently)
 * forbid modifying the query tuple expression ({@link #getExpression()}), THE ALGEBRAIC
 * EXPRESSION MUST NOT BE MODIFIED, as this will interfer with caching.
 * </p>
 * 
 * @param <T>
 *            the result type of the query, as bound to the query {@link QueryType}
 * 
 * @apiviz.has eu.fbk.dkm.springles.base.QueryType
 */
public final class QuerySpec<T> implements Serializable
{

    /** Version identification code for serialization. */
    private static final long serialVersionUID = -1700750865112307336L;

    /**
     * A cache keeping track of created instances, which may be reclaimed by the GC. Instances are
     * indexed based on a <tt>type, string, language, baseURI</tt> tuple.
     */
    private static final Cache<List<Object>, QuerySpec<?>> CACHE = CacheBuilder.newBuilder()
            .softValues().build();

    /** The query type. */
    private final QueryType<T> type;

    /** The query string. */
    private final String string;

    /** The language the query string is expressed in. */
    private final QueryLanguage language;

    /** The base URI for interpreting relative URIs in the query string, possibly null. */
    @Nullable
    private final String baseURI;

    /** Flag that is <tt>true</tt> if the query string was rendered. */
    private final boolean rendered;

    /** The algebraic tuple expression; null if the query cannot be parsed. */
    private final TupleExpr expression;

    /** The optional dataset associated to the query; null if it cannot be parsed. */
    @Nullable
    private final Dataset dataset;

    /** The parsed namespaces for the query; null if the query cannot be parsed. */
    private final Map<String, String> namespaces;

    /**
     * Returns a <tt>QuerySpec</tt> for the specified query type and SPARQL query string. This is
     * a convenience method corresponding to
     * <tt>from(type, string, QueryLanguage.SPARQL, null)</tt>.
     * 
     * @param type
     *            the query type
     * @param string
     *            the query string, in SPARQL and without relative URIs
     * @param <T>
     *            the result type of the query, as bound to the query type
     * @return the corresponding <tt>QuerySpec</tt>
     * @throws MalformedQueryException
     *             in case the string has syntax errors
     */
    public static <T> QuerySpec<T> from(final QueryType<T> type, final String string)
            throws MalformedQueryException
    {
        return from(type, string, QueryLanguage.SPARQL, null);
    }

    /**
     * Returns a <tt>QuerySpec</tt> for the supplied query type, string, language and optional
     * base URI.
     * 
     * @param type
     *            the query type
     * @param string
     *            the query string
     * @param language
     *            the language the query string is expressed in
     * @param baseURI
     *            the base URI to interpret relative URIs in the string, possibly null if relative
     *            URIs are not present
     * @param <T>
     *            the result type of the query, as bound to the query type
     * @return the corresponding <tt>QuerySpec</tt>
     * @throws MalformedQueryException
     *             in case the string has syntax errors
     */
    public static <T> QuerySpec<T> from(final QueryType<T> type, final String string,
            final QueryLanguage language, @Nullable final String baseURI)
            throws MalformedQueryException
    {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(string);
        Preconditions.checkNotNull(language);

        final List<Object> cacheKey = ImmutableList.of(type, string, language,
                Strings.nullToEmpty(baseURI));

        @SuppressWarnings("unchecked")
        QuerySpec<T> result = (QuerySpec<T>) CACHE.getIfPresent(cacheKey);

        if (result == null) {
            final QueryParserFactory factory = QueryParserRegistry.getInstance().get(language);

            if (factory == null) {
                result = new QuerySpec<T>(type, string, language, baseURI, false, null, null, null);

            } else {
                final ParsedQuery parsedQuery = QueryParserUtil.parseQuery(language, string,
                        baseURI);
                final QueryType<?> parsedType = QueryType.forParsedClass(parsedQuery.getClass());
                if (type != parsedType) {
                    throw new MalformedQueryException("Expected " + type + " query, got "
                            + parsedType + " query");
                }
                result = new QuerySpec<T>(type, string, language, baseURI, false,
                        parsedQuery.getTupleExpr(), parsedQuery.getDataset(),
                        parsedQuery instanceof ParsedGraphQuery ? ((ParsedGraphQuery) parsedQuery)
                                .getQueryNamespaces() : Collections.<String, String>emptyMap());
            }

            CACHE.put(cacheKey, result);
        }

        return result;
    }

    /**
     * Returns a <tt>QuerySpec</tt> for the query type, algebraic expressions and corresponding
     * dataset specified. This is a convenience methods corresponding to
     * <tt>from(type, expression, dataset, Collections.emptyMap())</tt>.
     * 
     * @param type
     *            the query type
     * @param expression
     *            the algebraic expression for the query
     * @param dataset
     *            the dataset corresponding to the algebraic expression, possibly null if no
     *            explicit dataset is associated to the query
     * @param <T>
     *            the result type of the query, as bound to the query type
     * @return the corresponding <tt>QuerySpec</tt>
     */
    public static <T> QuerySpec<T> from(final QueryType<T> type, final TupleExpr expression,
            @Nullable final Dataset dataset)
    {
        return from(type, expression, dataset, Collections.<String, String>emptyMap());
    }

    /**
     * Returns an <tt>QuerySpec</tt> for the query type, algebraic expression, corresponding
     * dataset and namespace declarations specified.
     * 
     * @param type
     *            the query type
     * @param expression
     *            the algebraic expression for the query
     * @param dataset
     *            the dataset corresponding to the algebraic expression, possibly null if no
     *            explicit dataset is associated to the query
     * @param namespaces
     *            the namespace declarations
     * @param <T>
     *            the result type of the query, as bound to the query type
     * @return the corresponding <tt>QuerySpec</tt>
     */
    public static <T> QuerySpec<T> from(final QueryType<T> type, final TupleExpr expression,
            @Nullable final Dataset dataset, final Map<String, String> namespaces)
    {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(expression);
        Preconditions.checkNotNull(namespaces);

        final Dataset clonedDataset = dataset == null ? null : Algebra.clone(dataset);

        final String string = SparqlRenderer.render(expression, clonedDataset)
                .withNamespaces(namespaces).toString();
        final List<Object> cacheKey = ImmutableList.of(type, string, QueryLanguage.SPARQL, "");

        @SuppressWarnings("unchecked")
        QuerySpec<T> result = (QuerySpec<T>) CACHE.getIfPresent(cacheKey);
        if (result == null) {
            result = new QuerySpec<T>(type, string, QueryLanguage.SPARQL, null, true, expression,
                    clonedDataset, namespaces);
            CACHE.put(cacheKey, result);
        }

        return result;
    }

    /**
     * Private constructor, accepting parameters for all the object properties.
     * 
     * @param type
     *            the query type
     * @param string
     *            the update string
     * @param language
     *            the language the update string is expressed in
     * @param baseURI
     *            the optional base URI, possibly null
     * @param rendered
     *            <tt>true</tt> if the update string was rendered
     * @param expression
     *            the algebraic expression, null if the query cannot be parsed
     * @param dataset
     *            the associated dataset, null if cannot be parsed or left unspecified
     * @param namespaces
     *            the namespace declarations, null if the query cannot be parsed
     */
    private QuerySpec(final QueryType<T> type, final String string, final QueryLanguage language,
            @Nullable final String baseURI, final boolean rendered,
            @Nullable final TupleExpr expression, @Nullable final Dataset dataset,
            @Nullable final Map<String, String> namespaces)
    {
        this.type = type;
        this.language = language;
        this.string = string;
        this.rendered = rendered;
        this.baseURI = baseURI;
        this.expression = expression;
        this.dataset = dataset;
        this.namespaces = namespaces == null ? null : ImmutableMap.copyOf(namespaces);
    }

    /**
     * Helper method that throws an {@link UnsupportedOperationException} if the algebraic
     * representation is not available.
     */
    private void checkParsed()
    {
        if (this.expression == null) {
            throw new UnsupportedOperationException("Operation unsupported for language "
                    + this.language);
        }
    }

    /**
     * Returns the query type.
     * 
     * @return the query type
     */
    public QueryType<T> getType()
    {
        return this.type;
    }

    /**
     * Returns the query string. The returned string is expressed in language
     * {@link #getLanguage()} and possibly automatically rendered ({@link #isRendered()}
     * <tt>true</tt>) from a supplied algebraic expression.
     * 
     * @return the update string
     */
    public String getString()
    {
        return this.string;
    }

    /**
     * Returns the language the query string is expressed in. If the string has been automatically
     * rendered from a supplied algebraic expression, the language is {@link QueryLanguage#SPARQL}
     * .
     * 
     * @return the language
     */
    public QueryLanguage getLanguage()
    {
        return this.language;
    }

    /**
     * Returns the optional base URI to be used to interpret relative URIs in the query string.
     * The base URI can be <tt>null</tt>, in which case it is assumed that the query string does
     * not contain relative URIs.
     * 
     * @return the base URI, possibly null
     */
    @Nullable
    public String getBaseURI()
    {
        return this.baseURI;
    }

    /**
     * Returns <tt>true</tt> if the query string has been rendered starting from a supplied
     * algebraic expression.
     * 
     * @return <tt>true</tt> if the query string has been rendered
     */
    public boolean isRendered()
    {
        return this.rendered;
    }

    /**
     * Returns <tt>true</tt> if the algebraic representation of the query is available. This
     * representation may be unavailable in case a query string has been supplied, whose language
     * has not an associated Sesame parser.
     * 
     * @return <tt>true</tt> if the algebraic representation is available
     */
    public boolean isParsed()
    {
        return this.expression != null;
    }

    /**
     * Returns the algebraic expression for the query - DON'T MODIFY THE RESULT. As a query
     * expression must be cached for performance reasons, modifying it would affect all subsequent
     * operations on the same <tt>QuerySpec</tt> object, so CLONE THE EXPRESSION BEFORE MODIFYING
     * IT.
     * 
     * @return the algebraic expression for this query.
     * @throws UnsupportedOperationException
     *             in case the algebraic representation is unavailable
     */
    public TupleExpr getExpression() throws UnsupportedOperationException
    {
        checkParsed();
        return this.expression;
    }

    /**
     * Returns the dataset associated to the algebraic expression, or <tt>null</tt> if such
     * dataset has not been explicitly specified.
     * 
     * @return the dataset, possibly null
     * @throws UnsupportedOperationException
     *             in case the algebraic representation is unavailable
     */
    @Nullable
    public Dataset getDataset() throws UnsupportedOperationException
    {
        checkParsed();
        return this.dataset;
    }

    /**
     * Returns the namespace declarations for the query.
     * 
     * @return a map of prefix -> namespace declarations
     * @throws UnsupportedOperationException
     *             in case the algebraic representation is unavailable
     */
    public Map<String, String> getNamespaces() throws UnsupportedOperationException
    {
        checkParsed();
        return this.namespaces;
    }

    /**
     * Returns a <tt>QuerySpec</tt> with the same query expression of this specification, but
     * associated to the dataset specified.
     * 
     * @param dataset
     *            the dataset to be enforced, <tt>null</tt> to associate to an unspecified dataset
     * @return the resulting <tt>QuerySpec</tt> object (possibly <tt>this</tt> if no change is
     *         required)
     * @throws UnsupportedOperationException
     *             in case the algebraic representation of <tt>this</tt> is unavailable
     */
    public QuerySpec<T> enforceDataset(final Dataset dataset) throws UnsupportedOperationException
    {
        checkParsed();

        if (Objects.equal(this.dataset, dataset)) {
            return this;
        } else {
            return from(this.type, this.expression, dataset, this.namespaces);
        }
    }

    /**
     * {@inheritDoc} Two instances are equal if they have the same string representation.
     */
    @Override
    public boolean equals(final Object object)
    {
        if (object == this) {
            return true;
        }
        if (object == null || !(object instanceof QuerySpec<?>)) {
            return false;
        }
        final QuerySpec<?> other = (QuerySpec<?>) object;
        return this.type == other.type && this.string.equals(other.string)
                && this.language.equals(other.language)
                && Objects.equal(this.baseURI, other.baseURI);
    }

    /**
     * {@inheritDoc} The returned hash code depends only on the string representation.
     */
    @Override
    public int hashCode()
    {
        return Objects.hashCode(this.type, this.language, this.string, this.baseURI);
    }

    /**
     * {@inheritDoc} Returns the query string.
     */
    @Override
    public String toString()
    {
        return this.string;
    }

    /**
     * Overrides deserialization, reusing cached <tt>QuerySpec</tt>s where possible.
     * 
     * @return the object to return as result of deserialization
     * @throws ObjectStreamException
     *             declared as mandated by serialization API
     */
    private Object readResolve() throws ObjectStreamException
    {
        final List<Object> cacheKey = ImmutableList.of(this.type, this.string, this.language,
                Strings.nullToEmpty(this.baseURI));
        QuerySpec<?> result = CACHE.getIfPresent(cacheKey);
        if (result == null) {
            result = this;
            CACHE.put(cacheKey, this);
        }
        return result;
    }

}
