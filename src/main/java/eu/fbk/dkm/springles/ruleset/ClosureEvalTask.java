package eu.fbk.dkm.springles.ruleset;

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;

import org.openrdf.model.BNode;
import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.ValueExpr;

import eu.fbk.dkm.internal.util.Selector;

public final class ClosureEvalTask extends ClosureTask
{

    private List<Resource> ruleIDs;

    // CONSTRUCTION

    public ClosureEvalTask()
    {
        this(null, null, null);
    }

    public ClosureEvalTask(@Nullable final Resource id,
            @Nullable final Map<? extends String, ? extends ValueExpr> bindings,
            @Nullable final Iterable<? extends Resource> ruleIDs)
    {
        super(id, bindings);
        this.ruleIDs = ruleIDs == null ? Lists.<Resource>newArrayList() : Lists
                .newArrayList(ruleIDs);
    }

    // PROPERTIES

    public List<Resource> getRuleIDs()
    {
        return this.ruleIDs;
    }

    public void setRuleIDs(@Nullable final Iterable<? extends Resource> ruleIDs)
    {
        checkMutable();
        this.ruleIDs = ruleIDs == null ? Lists.<Resource>newArrayList() : Lists
                .newArrayList(ruleIDs);
    }

    // SERIALIZATION AND DESERIALIZATION IN RDF

    @Override
    public Resource emitRDF(final Graph graph, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces)
    {
        final Resource id = super.emitRDF(graph, baseURI, namespaces);

        graph.add(id, RDF.TYPE, SPR.CLOSURE_EVAL_TASK);
        URI pred = SPR.EVAL_OF;
        Resource node = id;
        for (final Resource ruleID : this.ruleIDs) {
            final BNode newNode = graph.getValueFactory().createBNode();
            graph.add(node, pred, newNode);
            graph.add(newNode, RDF.FIRST, ruleID);
            pred = RDF.REST;
            node = newNode;
        }
        graph.add(node, pred, RDF.NIL);

        return id;
    }

    @Override
    public void parseRDF(final Graph graph, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces) throws MalformedQueryException
    {
        super.parseRDF(graph, baseURI, namespaces);
        final Selector s = Selector.select(graph, getID());
        if (!s.isEmpty(SPR.EVAL_OF)) {
            this.ruleIDs = s.getList(SPR.EVAL_OF, Resource.class);
        }
    }

    // VALIDATION

    @Override
    public void validate()
    {
        super.validate();
        validate(!this.ruleIDs.contains(null), "null rule ID");
        validate(this.ruleIDs.size() == Sets.newHashSet(this.ruleIDs).size(),
                "duplicate rule ID(s)");
    }

    // FREEZING AND CLONING

    @Override
    public void freeze()
    {
        if (!isFrozen()) {
            this.ruleIDs = ImmutableList.copyOf(this.ruleIDs);
            super.freeze();
        }
    }

    @Override
    public ClosureEvalTask clone()
    {
        final ClosureEvalTask clone = (ClosureEvalTask) super.clone();
        clone.ruleIDs = Lists.newArrayList(this.ruleIDs);
        return clone;
    }

    // COMPARISON AND HASHING

    @Override
    public boolean equals(@Nullable final Object object)
    {
        return super.equals(object) && ((ClosureEvalTask) object).ruleIDs.equals(this.ruleIDs);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(super.hashCode(), this.ruleIDs);
    }

    @Override
    protected void computeDigest(final Hasher hasher)
    {
        super.computeDigest(hasher);
        for (final Resource ruleID : this.ruleIDs) {
            hasher.putUnencodedChars(" ").putUnencodedChars(ruleID.stringValue());
        }
    }

}
