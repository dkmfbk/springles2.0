package eu.fbk.dkm.springles.base;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.eclipse.rdf4j.query.parser.QueryParserFactory;
import org.eclipse.rdf4j.query.parser.QueryParserRegistry;

import eu.fbk.dkm.internal.util.SparqlRenderer;

/**
 * An update operation specification.
 * <p>
 * This class models the specification of an update operation, combining in a single object both a
 * language-specific string representation (e.g., SPARQL) and the Sesame algebraic representation:
 * </p>
 * <ul>
 * <li>the string representation is encoded through properties {@link #getString()},
 * {@link #getLanguage()} and {@link #getBaseURI()}, the latter being the optional URI used to
 * interpret relative URIs in the update string;</li>
 * <li>the algebraic representation is encoded through properties {@link #getExpressions()},
 * {@link #getDatasets()} and {@link #getNamespaces()}; an update operation is a sequence of
 * statements each having its expression and dataset, which share the same namespace declarations.
 * </li>
 * </ul>
 * <p>
 * An <tt>UpdateSpec</tt> can be created in two ways: either providing its string representation
 * (methods {@link #from(String)} and {@link #from(String, QueryLanguage, String)}); or providing
 * its algebraic representation (methods {@link #from(Iterable, Iterable)} and
 * {@link #from(Iterable, Iterable, Map)}). In both case, the dual representation is automatically
 * derived, either via parsing or rendering to SPARQL language:
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
 * <tt>UpdateSpec</tt> objects are created through the <tt>from</tt> factory methods which provide
 * for the caching and reuse of already created expression, thus reducing parsing overhead.
 * Serialization is supported, with deserialization attempting to reuse existing objects from the
 * cache. Instances have to considered to be immutable: while it is not possible to (efficiently)
 * forbid modifying the update expressions ({@link #getExpressions()}), ALGEBRAIC EXPRESSIONS MUST
 * NOT BE MODIFIED, as this will interfer with caching.
 * </p>
 */
public final class UpdateSpec implements Serializable
{

    /** Version identification code for serialization. */
    private static final long serialVersionUID = 1599674557051690483L;

    /**
     * A cache keeping track of created instances, which may be reclaimed by the GC. Instances are
     * indexed based on a <tt>string, language, baseURI</tt> tuple.
     */
    private static final Cache<List<Object>, UpdateSpec> CACHE = CacheBuilder.newBuilder()
            .softValues().build();

    /** The update string. */
    private final String string;

    /** The language the update string is expressed in. */
    private final QueryLanguage language;

    /** The base URI for interpreting relative URIs in the update string, possibly null. */
    @Nullable
    private final String baseURI;

    /** Flag that is <tt>true</tt> if the update string was rendered. */
    private final boolean rendered;

    /** The parsed algebraic expressions for the update operation; null if it cannot be parsed. */
    @Nullable
    private final List<UpdateExpr> expressions;

    /** The parsed datasets for the update operation; null if it cannot be parsed. */
    @Nullable
    private final List<Dataset> datasets;

    /** The parsed namespaces for the update operation; null if it cannot be parsed. */
    @Nullable
    private final Map<String, String> namespaces;

    /**
     * Returns an <tt>UpdateSpec</tt> for the specified SPARQL update string. This is a
     * convenience method corresponding to <tt>from(string, QueryLanguage.SPARQL, null)</tt>.
     *
     * @param string
     *            the update string, in SPARQL and without relative URIs
     * @return the corresponding <tt>UpdateSpec</tt>
     * @throws MalformedQueryException
     *             in case the string has syntax errors
     */
    public static UpdateSpec from(final String string) throws MalformedQueryException
    {
        return UpdateSpec.from(string, QueryLanguage.SPARQL, null);
    }

    /**
     * Returns an <tt>UpdateSpec</tt> for the supplied update string, language and optional base
     * URI.
     *
     * @param string
     *            the update string
     * @param language
     *            the language the update string is expressed in
     * @param baseURI
     *            the base URI to interpret relative URIs in the string
     * @return the corresponding <tt>UpdateSpec</tt>
     * @throws MalformedQueryException
     *             in case the string has syntax errors
     */
    public static UpdateSpec from(final String string, final QueryLanguage language,
            @Nullable final String baseURI) throws MalformedQueryException
    {
        Preconditions.checkNotNull(string);
        Preconditions.checkNotNull(language);

        final List<Object> cacheKey = ImmutableList.of(string, language,
                Strings.nullToEmpty(baseURI));

        UpdateSpec result = UpdateSpec.CACHE.getIfPresent(cacheKey);

        if (result == null) {
            final QueryParserFactory factory = QueryParserRegistry.getInstance().get(language)
                    .get();

            if (factory == null) {
                result = new UpdateSpec(string, language, baseURI, false, null, null, null);

            } else {
                final ParsedUpdate parsedUpdate = factory.getParser().parseUpdate(string, baseURI);
                final List<UpdateExpr> exprs = parsedUpdate.getUpdateExprs();
                final Map<String, String> namespaces = parsedUpdate.getNamespaces();
                final List<Dataset> datasets = Lists
                        .newArrayListWithCapacity(parsedUpdate.getUpdateExprs().size());
                for (final UpdateExpr expr : parsedUpdate.getUpdateExprs()) {
                    datasets.add(parsedUpdate.getDatasetMapping().get(expr));
                }
                result = new UpdateSpec(string, language, baseURI, false, exprs, datasets,
                        namespaces);
            }

            UpdateSpec.CACHE.put(cacheKey, result);
        }

        return result;
    }

    /**
     * Returns an <tt>UpdateSpec</tt> for the algebraic expressions and corresponding datasets
     * specified. This is a convenience methods corresponding to
     * <tt>from(expressions, datasets, Collections.emptyMap())</tt>.
     *
     * @param expressions
     *            the algebraic expressions for the statements of the update operation
     * @param datasets
     *            the datasets corresponding to the algebraic expressions (must have the same
     *            size)
     * @return the corresponding <tt>UpdateSpec</tt>
     */
    public static UpdateSpec from(final Iterable<UpdateExpr> expressions,
            final Iterable<Dataset> datasets)
    {
        return UpdateSpec.from(expressions, datasets, Collections.<String, String>emptyMap());
    }

    /**
     * Returns an <tt>UpdateSpec</tt> for the algebraic expressions, the corresponding datasets
     * and the namespace declarations specified.
     *
     * @param expressions
     *            the algebraic expressions for the statements of the update operation
     * @param datasets
     *            the datasets corresponding to the algebraic expressions (must have the same
     *            size)
     * @param namespaces
     *            the namespace declarations
     * @return the corresponding <tt>UpdateSpec</tt>
     */
    public static UpdateSpec from(final Iterable<UpdateExpr> expressions,
            final Iterable<Dataset> datasets, final Map<String, String> namespaces)
    {
        Preconditions.checkNotNull(expressions);
        Preconditions.checkNotNull(datasets);
        Preconditions.checkArgument(Iterables.size(expressions) == Iterables.size(datasets));
        Preconditions.checkNotNull(namespaces);

        final String string = SparqlRenderer.render(expressions, datasets)
                .withNamespaces(namespaces).toString();

        final List<Object> cacheKey = ImmutableList.of(string, QueryLanguage.SPARQL, "");

        UpdateSpec result = UpdateSpec.CACHE.getIfPresent(cacheKey);
        if (result == null) {
            result = new UpdateSpec(string, QueryLanguage.SPARQL, null, true, expressions,
                    datasets, namespaces);
            UpdateSpec.CACHE.put(cacheKey, result);
        }

        return result;
    }

    /**
     * Private constructor, accepting parameters for all the object properties.
     *
     * @param string
     *            the update string
     * @param language
     *            the language the update string is expressed in
     * @param baseURI
     *            the optional base URI
     * @param rendered
     *            <tt>true</tt> if the update string was rendered
     * @param expressions
     *            the algebraic expressions, possibly null
     * @param datasets
     *            the corresponding datasets, possibly null
     * @param namespaces
     *            the namespace declarations, possibly null
     */
    private UpdateSpec(final String string, final QueryLanguage language,
            @Nullable final String baseURI, final boolean rendered,
            @Nullable final Iterable<UpdateExpr> expressions,
            @Nullable final Iterable<Dataset> datasets,
            @Nullable final Map<String, String> namespaces)
    {
        this.string = string;
        this.language = language;
        this.baseURI = baseURI;
        this.rendered = rendered;
        this.expressions = expressions == null ? null : ImmutableList.copyOf(expressions);
        this.datasets = datasets == null ? null
                : Collections.unmodifiableList(Lists.newArrayList(datasets));
        this.namespaces = namespaces == null ? null : ImmutableMap.copyOf(namespaces);
    }

    /**
     * Helper method that throws an {@link UnsupportedOperationException} if the algebraic
     * representation is not available.
     */
    private void checkParsed()
    {
        if (this.expressions == null) {
            throw new UnsupportedOperationException(
                    "Operation unsupported for language " + this.language);
        }
    }

    /**
     * Returns the update string. The returned string is expressed in language
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
     * Returns the language the update string is expressed in. If the string has been
     * automatically rendered from a supplied algebraic expression, the language is
     * {@link QueryLanguage#SPARQL}.
     *
     * @return the language
     */
    public QueryLanguage getLanguage()
    {
        return this.language;
    }

    /**
     * Returns the optional base URI to be used to interpret relative URIs in the update string.
     * The base URI can be <tt>null</tt>, in which case it is assumed that the update string does
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
     * Returns <tt>true</tt> if the update string has been rendered starting from a supplied
     * algebraic expression.
     *
     * @return <tt>true</tt> if the update string has been rendered
     */
    public boolean isRendered()
    {
        return this.rendered;
    }

    /**
     * Returns <tt>true</tt> if the algebraic representation of the update operation is available.
     * This representation may be unavailable in case an update string has been supplied, whose
     * language has not an associated Sesame parser.
     *
     * @return <tt>true</tt> if the algebraic representation is available
     */
    public boolean isParsed()
    {
        return this.expressions != null;
    }

    /**
     * Returns the algebraic expressions for the operations of the update operation - DON'T MODIFY
     * THE RESULT. As expressions must be cached for performance reasons, modifying them would
     * affect all subsequent operations reusing the same expressions, so CLONE THE EXPRESSIONS
     * BEFORE MODIFYING THEM.
     *
     * @return the algebraic expressions for this update operation
     * @throws UnsupportedOperationException
     *             in case the algebraic representation is unavailable
     */
    public List<UpdateExpr> getExpressions() throws UnsupportedOperationException
    {
        this.checkParsed();
        return this.expressions;
    }

    /**
     * Returns the datasets associated to the statements of the update operation.
     *
     * @return the datasets
     * @throws UnsupportedOperationException
     *             in case the algebraic representation is unavailable
     */
    public List<Dataset> getDatasets() throws UnsupportedOperationException
    {
        this.checkParsed();
        return this.datasets;
    }

    /**
     * Returns the namespace declarations for the update operation.
     *
     * @return a map of prefix -> namespace declarations
     * @throws UnsupportedOperationException
     *             in case the algebraic representation is unavailable
     */
    public Map<String, String> getNamespaces() throws UnsupportedOperationException
    {
        this.checkParsed();
        return this.namespaces;
    }

    /**
     * Splits a compound update operation into a list of atomic (single-statements) operations.
     *
     * @return a list of update operations, one for each update statement in this operation
     * @throws UnsupportedOperationException
     *             in case the algebraic representation is unavailable
     */
    public List<UpdateSpec> explode() throws UnsupportedOperationException
    {
        this.checkParsed();
        final int size = this.expressions.size();
        if (size == 1) {
            return Collections.singletonList(this);
        } else {
            final List<UpdateSpec> result = Lists.newArrayListWithCapacity(size);
            for (int i = 0; i < size; ++i) {
                result.add(UpdateSpec.from(Collections.singleton(this.expressions.get(i)),
                        Collections.singleton(this.datasets.get(i)), this.namespaces));
            }
            return result;
        }
    }

    /**
     * Returns an <tt>UpdateSpec</tt> with the same statements of this specification, all being
     * associated to the dataset specified.
     *
     * @param dataset
     *            the dataset to be enforced, <tt>null</tt> to associate to an unspecified dataset
     * @return the resulting <tt>UpdateSpec</tt> object (possibly <tt>this</tt> if no change is
     *         required)
     * @throws UnsupportedOperationException
     *             in case the algebraic representation of <tt>this</tt> is unavailable
     */
    public UpdateSpec enforceDataset(final Dataset dataset) throws UnsupportedOperationException
    {
        this.checkParsed();

        if (Iterables.all(this.datasets, Predicates.equalTo(dataset))) {
            return this;
        } else {
            return UpdateSpec.from(this.expressions,
                    Collections.nCopies(this.datasets.size(), dataset), this.namespaces);
        }
    }

    /**
     * {@inheritDoc} Two instances are equal if they have the same string representation.
     */
    @Override
    public boolean equals(@Nullable final Object object)
    {
        if (object == this) {
            return true;
        }
        if (object == null || !(object instanceof UpdateSpec)) {
            return false;
        }
        final UpdateSpec other = (UpdateSpec) object;
        return this.string.equals(other.string) && this.language.equals(other.language)
                && Objects.equal(this.baseURI, other.baseURI);
    }

    /**
     * {@inheritDoc} The returned hash code depends only on the string representation.
     */
    @Override
    public int hashCode()
    {
        return Objects.hashCode(this.language, this.string, this.baseURI);
    }

    /**
     * {@inheritDoc} Returns the update string.
     */
    @Override
    public String toString()
    {
        return this.string;
    }

    /**
     * Overrides deserialization, reusing cached <tt>UpdateSpec</tt>s where possible.
     *
     * @return the object to return as result of deserialization
     * @throws ObjectStreamException
     *             declared as mandated by serialization API
     */
    private Object readResolve() throws ObjectStreamException
    {
        final List<Object> cacheKey = ImmutableList.of(this.string, this.language,
                Strings.nullToEmpty(this.baseURI));
        UpdateSpec result = UpdateSpec.CACHE.getIfPresent(cacheKey);
        if (result == null) {
            result = this;
            UpdateSpec.CACHE.put(cacheKey, this);
        }
        return result;
    }

}
