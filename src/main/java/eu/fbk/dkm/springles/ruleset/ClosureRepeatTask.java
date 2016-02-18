package eu.fbk.dkm.springles.ruleset;

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.hash.Hasher;

import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.ValueExpr;

import eu.fbk.dkm.internal.util.Algebra;
import eu.fbk.dkm.internal.util.Selector;
import eu.fbk.dkm.internal.util.SparqlRenderer;
import eu.fbk.dkm.springles.base.QuerySpec;
import eu.fbk.dkm.springles.base.QueryType;

public final class ClosureRepeatTask extends ClosureTask
{

    @Nullable
    private ClosureTask subTask;

    @Nullable
    private QuerySpec<TupleQueryResult> query;

    // CONSTRUCTION

    public ClosureRepeatTask()
    {
        this(null, null, null, null);
    }

    public ClosureRepeatTask(@Nullable final Resource id,
            @Nullable final Map<? extends String, ? extends ValueExpr> bindings,
            @Nullable final ClosureTask subTask, @Nullable final QuerySpec<TupleQueryResult> query)
    {
        super(id, bindings);
        this.subTask = subTask;
        this.query = query;
    }

    // PROPERTIES

    @Nullable
    public ClosureTask getSubTask()
    {
        return this.subTask;
    }

    public void setSubTask(@Nullable final ClosureTask subTask)
    {
        checkMutable();
        this.subTask = subTask;
    }

    @Nullable
    public QuerySpec<TupleQueryResult> getQuery()
    {
        return this.query;
    }

    public void setQuery(@Nullable final QuerySpec<TupleQueryResult> query)
    {
        checkMutable();
        this.query = query;
    }

    // SERIALIZATION AND DESERIALIZATION IN RDF

    @Override
    public Resource emitRDF(final Graph graph, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces)
    {
        final Resource id = super.emitRDF(graph, baseURI, namespaces);
        graph.add(id, RDF.TYPE, SPR.CLOSURE_REPEAT_TASK);
        if (this.subTask != null) {
            graph.add(id, SPR.REPEAT_OF, this.subTask.emitRDF(graph, baseURI, namespaces));
        }
        if (this.query != null) {
            final String expr = SparqlRenderer.render(this.query.getExpression())
                    .withNamespaces(namespaces).withHeader(false).toString();
            graph.add(id, SPR.REPEAT_OF,
                    graph.getValueFactory().createLiteral(expr, XMLSchema.STRING));
        }
        return id;
    }

    @Override
    public void parseRDF(final Graph graph, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces) throws MalformedQueryException
    {
        super.parseRDF(graph, baseURI, namespaces);
        final Selector s = Selector.select(graph, getID());
        final String query = s.get(SPR.REPEAT_OVER, String.class, null);
        final Resource subTaskID = s.get(SPR.REPEAT_OF, Resource.class, null);
        if (query != null) {
            this.query = QuerySpec.from(QueryType.TUPLE,
                    Algebra.parseTupleExpr(query, baseURI, namespaces), null, namespaces);
        }
        if (subTaskID != null) {
            this.subTask = ClosureTask.parseRDF(graph, baseURI, namespaces, subTaskID);
        }
    }

    // VALIDATION

    @Override
    public void validate()
    {
        super.validate();
        validate(this.subTask != null, "missing mandatory sub-task");
        validate(this.query != null, "missing mandatory query");
        this.subTask.validate();
    }

    // FREEZING AND CLONING

    @Override
    public void freeze()
    {
        if (!isFrozen()) {
            if (this.subTask != null) {
                this.subTask.freeze();
            }
            super.freeze();
        }
    }

    @Override
    public ClosureRepeatTask clone()
    {
        final ClosureRepeatTask clone = (ClosureRepeatTask) super.clone();
        if (this.subTask != null) {
            clone.subTask = this.subTask.clone();
        }
        return clone;
    }

    // COMPARISON AND HASHING

    @Override
    public boolean equals(@Nullable final Object object)
    {
        return super.equals(object)
                && Objects.equal(this.subTask, ((ClosureRepeatTask) object).subTask)
                && Objects.equal(this.query, ((ClosureRepeatTask) object).query);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(super.hashCode(), this.subTask, this.query);
    }

    @Override
    protected void computeDigest(final Hasher hasher)
    {
        super.computeDigest(hasher);
        hasher.putUnencodedChars(this.query == null ? " " : this.query.getString());
        hasher.putUnencodedChars(this.subTask == null ? " " : this.subTask.digest());
    }

}
