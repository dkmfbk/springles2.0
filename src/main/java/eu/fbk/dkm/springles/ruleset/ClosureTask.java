package eu.fbk.dkm.springles.ruleset;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.algebra.ValueExpr;

import eu.fbk.dkm.internal.util.Algebra;
import eu.fbk.dkm.internal.util.Selector;
import eu.fbk.dkm.internal.util.SparqlRenderer;

public abstract class ClosureTask implements Cloneable
{

    @Nullable
    private Resource id;

    private Map<String, ValueExpr> bindings;

    private boolean frozen;

    @Nullable
    private transient String digest;

    // CONSTRUCTION

    protected ClosureTask()
    {
        this(null, null);
    }

    protected ClosureTask(@Nullable final Resource id,
            @Nullable final Map<? extends String, ? extends ValueExpr> bindings)
    {
        this.id = id;
        this.bindings = bindings == null ? Maps.<String, ValueExpr>newHashMap()
                : Maps.newHashMap(bindings);
        this.frozen = false;
    }

    // PROPERTIES

    @Nullable
    public final Resource getID()
    {
        return this.id;
    }

    public final void setID(@Nullable final Resource id)
    {
        this.checkMutable();
        this.id = id;
    }

    public final Map<String, ValueExpr> getBindings()
    {
        return this.bindings;
    }

    public final void setBindings(
            @Nullable final Map<? extends String, ? extends ValueExpr> bindings)
    {
        Preconditions.checkNotNull(bindings);
        this.checkMutable();
        this.bindings = bindings == null ? Maps.<String, ValueExpr>newHashMap()
                : Maps.newHashMap(bindings);
    }

    // SERIALIZATION AND DESERIALIZATION IN RDF

    public Resource emitRDF(final Model graph, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces)
    {
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Resource id = this.id != null ? this.id : vf.createBNode();

        for (final Map.Entry<String, ValueExpr> entry : this.bindings.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                final String expr = SparqlRenderer.render(entry.getValue())
                        .withNamespaces(namespaces).withHeader(false).toString();
                final Literal binding = vf.createLiteral("?" + entry.getKey() + " = " + expr,
                        XMLSchema.STRING);
                graph.add(id, SPR.BIND, binding);
            }
        }

        return id;
    }

    // ID must be assigned in order for the method to work

    public void parseRDF(final Model graph, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces) throws MalformedQueryException
    {
        this.checkMutable();
        Preconditions.checkNotNull(graph);
        Preconditions.checkState(this.id != null);

        final Selector s = Selector.select(graph, this.id);
        for (final String binding : s.getAll(SPR.BIND, String.class)) {
            final int index = binding.indexOf('=');
            if (index < 0) {
                throw new MalformedQueryException("Invalid bind expression in '" + this
                        + "' (missing equal sign):\n" + binding);
            }
            String var = binding.substring(0, index).trim();
            var = var.startsWith("?") ? var.substring(1) : var;
            try {
                final ValueExpr valueExpr = Algebra.parseValueExpr(binding.substring(index + 1),
                        baseURI, namespaces);
                this.bindings.put(var, valueExpr);
            } catch (final Throwable ex) {
                throw new MalformedQueryException(
                        "Invalid bind expression in '" + this + "':\n" + binding, ex);
            }
        }
    }

    public static ClosureTask parseRDF(final Model graph, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces, final Resource taskID)
            throws MalformedQueryException
    {
        final Resource id = taskID != null ? taskID
                : Iterables
                        .getOnlyElement(Iterables.concat(
                                graph.filter(null, RDF.TYPE, SPR.CLOSURE_EVAL_TASK),
                                graph.filter(null, RDF.TYPE, SPR.CLOSURE_FIX_POINT_TASK),
                                graph.filter(null, RDF.TYPE, SPR.CLOSURE_REPEAT_TASK),
                                graph.filter(null, RDF.TYPE, SPR.CLOSURE_SEQUENCE_TASK)))
                        .getSubject();

        final Selector s = Selector.select(graph, id);
        final Set<IRI> properties = s.properties();
        properties.retainAll(
                ImmutableSet.of(SPR.EVAL_OF, SPR.FIX_POINT_OF, SPR.REPEAT_OF, SPR.SEQUENCE_OF));
        Preconditions.checkArgument(properties.size() == 1);
        final IRI property = properties.iterator().next();

        ClosureTask task;
        if (property.equals(SPR.EVAL_OF)) {
            task = new ClosureEvalTask();
        } else if (property.equals(SPR.FIX_POINT_OF)) {
            task = new ClosureFixPointTask();
        } else if (property.equals(SPR.REPEAT_OF)) {
            task = new ClosureRepeatTask();
        } else if (property.equals(SPR.SEQUENCE_OF)) {
            task = new ClosureSequenceTask();
        } else {
            throw new IllegalArgumentException("Unknown closure task for property: " + property);
        }

        task.setID(id);
        task.parseRDF(graph, baseURI, namespaces);

        return task;
    }

    // VALIDATION

    public void validate()
    {
        this.validate(this.id != null, "missing ID");
        this.validate(!this.bindings.containsKey(null), "null binding variable");
        this.validate(!this.bindings.containsValue(null), "null binding expression");
    }

    protected final void validate(final boolean condition, final String message)
    {
        if (!condition) {
            throw new IllegalStateException("Invalid " + this.getClass().getSimpleName()
                    + (this.id == null ? "" : " " + this.id) + ": " + message);
        }
    }

    // FREEZING AND CLONING

    public final boolean isFrozen()
    {
        return this.frozen;
    }

    public void freeze()
    {
        if (!this.frozen) {
            this.frozen = true;
            this.bindings = ImmutableMap.copyOf(this.bindings);
        }
    }

    @Override
    public ClosureTask clone()
    {
        try {
            final ClosureTask node = (ClosureTask) super.clone();
            node.frozen = false;
            node.bindings = Maps.newHashMap(this.bindings);
            return node;

        } catch (final CloneNotSupportedException ex) {
            throw new Error("Unexpected exception: " + ex.getMessage(), ex);
        }
    }

    protected final void checkMutable()
    {
        if (this.frozen) {
            throw new IllegalStateException(
                    "Cannot modify frozen " + this.getClass().getSimpleName()
                            + (this.id == null ? "" : " " + this.id) + "; must clone it before");
        }
    }

    // COMPARISON AND HASHING

    @Override
    public boolean equals(@Nullable final Object object)
    {
        if (object == this) {
            return true;
        }
        if (object == null || object.getClass() == this.getClass()) {
            return false;
        }
        final ClosureTask other = (ClosureTask) object;
        return Objects.equal(this.id, other.id) && this.bindings.equals(other.bindings);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(this.id, this.bindings);
    }

    public final String digest()
    {
        String digest = this.digest;
        if (digest == null) {
            final Hasher hasher = Hashing.md5().newHasher();
            this.computeDigest(hasher);
            digest = hasher.hash().toString();
            this.digest = this.frozen ? digest : null;
        }
        return digest;
    }

    protected void computeDigest(final Hasher hasher)
    {
        hasher.putUnencodedChars(this.id instanceof IRI ? this.id.stringValue() : " ");
        for (final String name : Ordering.natural().sortedCopy(this.bindings.keySet())) {
            final ValueExpr expr = this.bindings.get(name);
            hasher.putUnencodedChars(name).putUnencodedChars("=");
            hasher.putUnencodedChars(expr == null ? " " : SparqlRenderer.render(expr).toString());
        }
    }

    // STRING REPRESENTATION

    @Override
    public final String toString()
    {
        final StringBuilder builder = new StringBuilder();
        builder.append(this.getClass().getSimpleName().substring("Closure".length()));
        if (this.id instanceof IRI) {
            builder.append(" ").append(((IRI) this.id).getLocalName());
        }
        return builder.toString();
    }

}
