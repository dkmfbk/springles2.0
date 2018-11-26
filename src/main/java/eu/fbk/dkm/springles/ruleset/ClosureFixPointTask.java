package eu.fbk.dkm.springles.ruleset;

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.hash.Hasher;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.algebra.ValueExpr;

import eu.fbk.dkm.internal.util.Selector;

public final class ClosureFixPointTask extends ClosureTask
{

    @Nullable
    private ClosureTask subTask;

    // CONSTRUCTION

    public ClosureFixPointTask()
    {
        this(null, null, null);
    }

    public ClosureFixPointTask(@Nullable final Resource id,
            @Nullable final Map<? extends String, ? extends ValueExpr> bindings,
            @Nullable final ClosureTask subTask)
    {
        super(id, bindings);
        this.subTask = subTask;
    }

    // PROPERTIES

    @Nullable
    public ClosureTask getSubTask()
    {
        return this.subTask;
    }

    public void setSubTask(@Nullable final ClosureTask subTask)
    {
        this.checkMutable();
        this.subTask = subTask;
    }

    // SERIALIZATION AND DESERIALIZATION IN RDF

    @Override
    public Resource emitRDF(final Model graph, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces)
    {
        final Resource id = super.emitRDF(graph, baseURI, namespaces);
        graph.add(id, RDF.TYPE, SPR.CLOSURE_FIX_POINT_TASK);
        if (this.subTask != null) {
            graph.add(id, SPR.FIX_POINT_OF, this.subTask.emitRDF(graph, baseURI, namespaces));
        }
        return id;
    }

    @Override
    public void parseRDF(final Model graph, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces) throws MalformedQueryException
    {
        super.parseRDF(graph, baseURI, namespaces);
        final Selector s = Selector.select(graph, this.getID());
        final Resource subTaskID = s.get(SPR.FIX_POINT_OF, Resource.class, null);
        if (subTaskID != null) {
            this.subTask = ClosureTask.parseRDF(graph, baseURI, namespaces, subTaskID);
        }
    }

    // VALIDATION

    @Override
    public void validate()
    {
        super.validate();
        this.validate(this.subTask != null, "missing mandatory sub-task");
        this.subTask.validate();
    }

    // FREEZING AND CLONING

    @Override
    public void freeze()
    {
        if (!this.isFrozen()) {
            if (this.subTask != null) {
                this.subTask.freeze();
            }
            super.freeze();
        }
    }

    @Override
    public ClosureFixPointTask clone()
    {
        final ClosureFixPointTask clone = (ClosureFixPointTask) super.clone();
        clone.subTask = this.subTask == null ? null : this.subTask.clone();
        return clone;
    }

    // COMPARISON AND HASHING

    @Override
    public boolean equals(@Nullable final Object object)
    {
        return super.equals(object)
                && Objects.equal(this.subTask, ((ClosureFixPointTask) object).subTask);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(super.hashCode(), this.subTask);
    }

    @Override
    protected void computeDigest(final Hasher hasher)
    {
        super.computeDigest(hasher);
        hasher.putUnencodedChars(this.subTask == null ? " " : this.subTask.digest());
    }

}
