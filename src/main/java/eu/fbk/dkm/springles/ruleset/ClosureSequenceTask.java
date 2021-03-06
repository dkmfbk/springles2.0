package eu.fbk.dkm.springles.ruleset;

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.hash.Hasher;

import org.openrdf.model.BNode;
import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.ValueExpr;

import eu.fbk.dkm.internal.util.Selector;

public final class ClosureSequenceTask extends ClosureTask
{

    private List<ClosureTask> subTasks;

    // CONSTRUCTION

    public ClosureSequenceTask()
    {
        this(null, null, null);
    }

    public ClosureSequenceTask(@Nullable final Resource id,
            @Nullable final Map<? extends String, ? extends ValueExpr> bindings,
            @Nullable final Iterable<? extends ClosureTask> subTasks)
    {
        super(id, bindings);
        this.subTasks = subTasks == null ? Lists.<ClosureTask>newArrayList() : Lists
                .newArrayList(subTasks);
    }

    // PROPERTIES

    public List<ClosureTask> getSubTasks()
    {
        return this.subTasks;
    }

    public void setSubTasks(@Nullable final Iterable<? extends ClosureTask> subTasks)
    {
        checkMutable();
        this.subTasks = subTasks == null ? Lists.<ClosureTask>newArrayList() : Lists
                .newArrayList(subTasks);
    }

    // SERIALIZATION AND DESERIALIZATION IN RDF

    @Override
    public Resource emitRDF(final Graph graph, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces)
    {
        final Resource id = super.emitRDF(graph, baseURI, namespaces);
        URI pred = SPR.SEQUENCE_OF;
        Resource node = id;
        graph.add(id, RDF.TYPE, SPR.CLOSURE_SEQUENCE_TASK);
        for (final ClosureTask subTask : this.subTasks) {
            final BNode newNode = graph.getValueFactory().createBNode();
            graph.add(node, pred, newNode);
            graph.add(newNode, RDF.FIRST, subTask.emitRDF(graph, baseURI, namespaces));
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
        if (!s.isEmpty(SPR.SEQUENCE_OF)) {
            this.subTasks.clear();
            for (final Resource subTaskID : s.getList(SPR.SEQUENCE_OF, Resource.class)) {
                this.subTasks.add(ClosureTask.parseRDF(graph, baseURI, namespaces, subTaskID));
            }
        }
    }

    // VALIDATION

    @Override
    public void validate()
    {
        super.validate();
        for (final ClosureTask node : this.subTasks) {
            validate(node != null, "null sub-task listed");
            node.validate();
        }
    }

    // FREEZING AND CLONING

    @Override
    public void freeze()
    {
        if (!isFrozen()) {
            this.subTasks = ImmutableList.copyOf(this.subTasks);
            for (final ClosureTask node : this.subTasks) {
                node.freeze();
            }
            super.freeze();
        }
    }

    @Override
    public ClosureSequenceTask clone()
    {
        final ClosureSequenceTask clone = (ClosureSequenceTask) super.clone();
        clone.subTasks = Lists.newArrayListWithCapacity(this.subTasks.size());
        for (final ClosureTask node : this.subTasks) {
            clone.subTasks.add(node.clone());
        }
        return clone;
    }

    // COMPARISON AND HASHING

    @Override
    public boolean equals(@Nullable final Object object)
    {
        return super.equals(object)
                && ((ClosureSequenceTask) object).subTasks.equals(this.subTasks);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(super.hashCode(), this.subTasks);
    }

    @Override
    protected void computeDigest(final Hasher hasher)
    {
        super.computeDigest(hasher);
        for (final ClosureTask subTask : this.subTasks) {
            hasher.putUnencodedChars(" ").putUnencodedChars(subTask.digest());
        }
    }

}
