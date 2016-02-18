package eu.fbk.dkm.springles.ruleset;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.openrdf.model.BNode;
import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;

import info.aduna.iteration.CloseableIteration;

import eu.fbk.dkm.internal.util.Algebra;
import eu.fbk.dkm.internal.util.Selector;
import eu.fbk.dkm.internal.util.SparqlRenderer;
import eu.fbk.dkm.springles.base.QuerySpec;
import eu.fbk.dkm.springles.base.QueryType;

public final class Rule implements Cloneable
{

    @Nullable
    private Resource id;

    @Nullable
    private TupleExpr head;

    @Nullable
    private TupleExpr body;

    @Nullable
    private ValueExpr condition;

    @Nullable
    private FunctionCall transform;

    @Nullable
    private Set<Resource> triggeredRuleIDs;

    private boolean frozen;

    @Nullable
    private transient String digest;

    @Nullable
    private transient Set<String> headVars;

    @Nullable
    private transient List<StatementPattern> headAtoms;

    @Nullable
    private transient Set<String> bodyVars;

    @Nullable
    private transient List<StatementPattern> bodyAtoms;

    @Nullable
    private transient List<ValueExpr> bodyConditions;

    @Nullable
    private transient QuerySpec<TupleQueryResult> bodyQuery;

    public Rule()
    {
        this(null, null, null, null, null, null);
    }

    public Rule(@Nullable final Resource id, @Nullable final TupleExpr head,
            @Nullable final TupleExpr body, @Nullable final ValueExpr condition,
            @Nullable final FunctionCall transform,
            @Nullable final Iterable<? extends Resource> triggeredRuleIDs)
    {
        this.id = id;
        this.head = head;
        this.body = body;
        this.condition = condition;
        this.transform = transform;
        this.triggeredRuleIDs = triggeredRuleIDs == null ? null : Sets
                .newHashSet(triggeredRuleIDs);
        this.frozen = false;

        this.headVars = null;
        this.headAtoms = null;
        this.bodyVars = null;
        this.bodyAtoms = null;
        this.bodyConditions = null;
        this.bodyQuery = null;
    }

    // PROPERTIES (SETTABLE AND DERIVED)

    @Nullable
    public Resource getID()
    {
        return this.id;
    }

    public void setID(@Nullable final Resource id)
    {
        checkMutable();
        this.id = id;
    }

    @Nullable
    public TupleExpr getHead()
    {
        return this.head;
    }

    public void setHead(@Nullable final TupleExpr head)
    {
        checkMutable();

        if (!Objects.equal(head, this.head)) {
            this.head = head;
            this.headVars = null;
            this.headAtoms = null;
        }
    }

    public Set<String> getHeadVars()
    {
        if (this.headVars == null && this.head != null) {
            this.headVars = Algebra.extractVariables(this.head);
        }
        return this.headVars;
    }

    public List<StatementPattern> getHeadAtoms()
    {
        if (this.headAtoms == null && this.head != null) {
            this.headAtoms = Algebra.extractPatterns(this.head);
        }
        return this.headAtoms;
    }

    @Nullable
    public TupleExpr getBody()
    {
        return this.body;
    }

    public void setBody(@Nullable final TupleExpr body)
    {
        checkMutable();

        if (!Objects.equal(body, this.body)) {
            this.body = body;
            this.bodyVars = null;
            this.bodyAtoms = null;
            this.bodyConditions = null;
            this.bodyQuery = null;
        }
    }

    public Set<String> getBodyVars()
    {
        if (this.bodyVars == null && this.body != null) {
            this.bodyVars = Algebra.extractVariables(this.body);
        }
        return this.bodyVars;
    }

    public List<StatementPattern> getBodyAtoms()
    {
        if (this.bodyAtoms == null && this.body != null) {
            this.bodyAtoms = Algebra.extractPatterns(this.body);
        }
        return this.bodyAtoms;
    }

    public List<ValueExpr> getBodyConditions()
    {
        if (this.bodyConditions == null && this.body != null) {
            this.bodyConditions = Algebra.extractConditions(this.body);
        }
        return this.bodyConditions;
    }

    @Nullable
    public QuerySpec<TupleQueryResult> getBodyQuery()
    {
        if (this.bodyQuery == null && this.body != null) {
            final Set<String> headVars = getHeadVars();
            final Set<String> projectionVars = Sets.newHashSet();
            for (final String var : getBody().getBindingNames()) {
                // TODO improve here
                if (var.startsWith("_emit") || headVars.contains(var) || this.transform != null) {
                    projectionVars.add(var);
                }
            }
            final List<ProjectionElem> projections = Lists.newArrayList();
            for (final String var : projectionVars) {
                projections.add(new ProjectionElem(var));
            }
            final TupleExpr expr = new Projection(this.body, new ProjectionElemList(projections));
            this.bodyQuery = QuerySpec.from(QueryType.TUPLE, expr, null);
        }
        return this.bodyQuery;
    }

    @Nullable
    public ValueExpr getCondition()
    {
        return this.condition;
    }

    public void setCondition(@Nullable final ValueExpr condition)
    {
        checkMutable();
        this.condition = condition;
    }

    @Nullable
    public FunctionCall getTransform()
    {
        return this.transform;
    }

    public void setTransform(@Nullable final FunctionCall transform)
    {
        checkMutable();
        this.transform = transform;
    }

    @Nullable
    public Set<Resource> getTriggeredRuleIDs()
    {
        return this.triggeredRuleIDs;
    }

    public void setTriggeredRuleIDs(@Nullable final Iterable<? extends Resource> triggeredRuleIDs)
    {
        checkMutable();
        this.triggeredRuleIDs = triggeredRuleIDs == null ? null : Sets
                .newHashSet(triggeredRuleIDs);
    }

    // INFERENCE GENERATION

    private TupleQueryResult transformIfNecessary(final TupleQueryResult iteration,
            final BindingSet bindings) throws QueryEvaluationException
    {
        if (this.transform == null) {
            return iteration;
        } else {
            final Value[] arguments = new Value[this.transform.getArgs().size()];
            for (int i = 0; i < arguments.length; ++i) {
                arguments[i] = Algebra.evaluateValueExpr(this.transform.getArgs().get(i),
                        bindings, ValueFactoryImpl.getInstance());
            }
            return Transform.transform(iteration, this.transform.getURI(), arguments);
        }
    }

    private int[][] setupMappingParameters(final TupleQueryResult iteration,
            final BindingSet bindings, final List<Value> values) throws QueryEvaluationException
    {
        final List<String> queryNames = Lists.newArrayList(iteration.getBindingNames());
        final List<String> allNames = Lists.newArrayList(queryNames);

        values.clear();
        values.addAll(Collections.nCopies(queryNames.size(), (Value) null));

        allNames.addAll(bindings.getBindingNames());
        for (final String name : bindings.getBindingNames()) {
            values.add(bindings.getValue(name));
        }

        final List<StatementPattern> patterns = getHeadAtoms();
        final int[][] indexes = new int[patterns.size()][];
        for (int i = 0; i < patterns.size(); ++i) {
            final List<Var> vars = patterns.get(i).getVarList();
            indexes[i] = new int[4 + 1];
            for (int j = 0; j < 4; ++j) {
                final Var var = vars.get(j);
                if (var.hasValue()) {
                    indexes[i][j] = values.size();
                    values.add(var.getValue());
                } else {
                    indexes[i][j] = allNames.indexOf(var.getName());
                }
            }
            indexes[i][4] = queryNames.indexOf("_emit" + (i + 1));
        }

        return indexes;
    }

    public <E extends Exception> void collectHeadStatements(final TupleQueryResult iteration,
            final BindingSet bindings, final StatementHandler<E> handler) throws E,
            QueryEvaluationException
    {
        final TupleQueryResult actualIteration = transformIfNecessary(iteration, bindings);

        final List<String> names = actualIteration.getBindingNames();
        final int numNames = names.size();
        final List<Value> values = Lists.newArrayList();
        final int[][] indexes = setupMappingParameters(actualIteration, bindings, values);
        final Value[] tuple = values.toArray(new Value[values.size()]);

        while (actualIteration.hasNext()) {
            final BindingSet queryBindings = actualIteration.next();
            for (int i = 0; i < numNames; ++i) {
                tuple[i] = queryBindings.getValue(names.get(i));
            }

            for (int row = 0; row < indexes.length; ++row) {

                final int[] offsets = indexes[row];

                if (offsets[4] >= 0) {
                    final Value emit = tuple[offsets[4]];
                    if (!(emit instanceof Literal) || !((Literal) emit).booleanValue()) {
                        continue;
                    }
                }
try{
                final Value subj = tuple[offsets[0]];
                final Value pred = tuple[offsets[1]];
                final Value obj = tuple[offsets[2]];
                final Value ctx = tuple[offsets[3]];

                if (subj instanceof Resource && pred instanceof URI && obj != null
                        && (ctx == null || ctx instanceof Resource)) {
                    handler.handle((Resource) subj, (URI) pred, obj, (Resource) ctx);
                }
                
}catch(ArrayIndexOutOfBoundsException e){
	System.out.println(e);
}
            }
        }
    }

    public CloseableIteration<Statement, QueryEvaluationException> generateHeadStatements(
            final TupleQueryResult iteration, final BindingSet bindings,
            final ValueFactory valueFactory) throws QueryEvaluationException
    {
        final TupleQueryResult actualIteration = transformIfNecessary(iteration, bindings);

        final List<String> names = actualIteration.getBindingNames();
        final int numNames = names.size();
        final List<Value> values = Lists.newArrayList();
        final int[][] indexes = setupMappingParameters(actualIteration, bindings, values);
        final Value[] tuple = values.toArray(new Value[values.size()]);

        return new CloseableIteration<Statement, QueryEvaluationException>() {

            private int row = indexes.length;

            private Statement next = null;

            @Override
            public boolean hasNext() throws QueryEvaluationException
            {
                if (this.next != null) {
                    return true;
                }

                while (true) {
                    if (this.row >= indexes.length) {
                        if (!iteration.hasNext()) {
                            return false;
                        }
                        final BindingSet bindings = iteration.next();
                        for (int i = 0; i < numNames; ++i) {
                            tuple[i] = bindings.getValue(names.get(i));
                        }
                        this.row = 0;
                    }

                    final int[] offsets = indexes[this.row++];

                    if (offsets[4] >= 0) {
                        final Value emit = tuple[offsets[4]];
                        if (!(emit instanceof Literal) || !((Literal) emit).booleanValue()) {
                            continue;
                        }
                    }

                    final Value subject = tuple[offsets[0]];
                    final Value predicate = tuple[offsets[1]];
                    final Value object = tuple[offsets[2]];
                    final Value context = tuple[offsets[3]];

                    if (subject instanceof Resource && predicate instanceof URI && object != null) {
                        if (context == null) {
                            this.next = valueFactory.createStatement((Resource) subject,
                                    (URI) predicate, object);
                            return true;

                        } else if (context instanceof Resource) {
                            this.next = valueFactory.createStatement((Resource) subject,
                                    (URI) predicate, object, (Resource) context);
                            return true;
                        }
                    }
                }
            }

            @Override
            public Statement next() throws QueryEvaluationException
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                final Statement result = this.next;
                this.next = null;
                return result;
            }

            @Override
            public void remove() throws QueryEvaluationException
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() throws QueryEvaluationException
            {
                iteration.close();
            }

        };
    }

    // SERIALIZATION AND DESERIALIZATION IN RDF

    public Resource emitRDF(final Graph graph, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces)
    {
        final ValueFactory vf = graph.getValueFactory();
        final Resource id = this.id != null ? this.id : vf.createBNode();

        graph.add(id, RDF.TYPE, SPR.RULE);

        if (this.triggeredRuleIDs != null) {
            URI pred = SPR.TRIGGER_OF;
            Resource node = id;
            for (final Resource triggeredRuleID : this.triggeredRuleIDs) {
                final BNode newNode = graph.getValueFactory().createBNode();
                graph.add(node, pred, newNode);
                graph.add(newNode, RDF.FIRST, triggeredRuleID);
                pred = RDF.REST;
                node = newNode;
            }
            graph.add(node, pred, RDF.NIL);
        }

        String expr;
        if (this.head != null) {
            expr = SparqlRenderer.render(this.head)
                    .withNamespaces(namespaces).toString();
            graph.add(id, SPR.HEAD, vf.createLiteral(expr, XMLSchema.STRING));
        }
        if (this.body != null) {
            expr = SparqlRenderer.render(this.body)
                    .withNamespaces(namespaces).toString();
            graph.add(id, SPR.BODY, vf.createLiteral(expr, XMLSchema.STRING));
        }
        if (this.condition != null) {
            expr = SparqlRenderer.render(this.condition)
                    .withNamespaces(namespaces).toString();
            graph.add(id, SPR.CONDITION, vf.createLiteral(expr, XMLSchema.STRING));
        }
        if (this.transform != null) {
            expr = SparqlRenderer.render(this.transform)
                    .withNamespaces(namespaces).toString();
            graph.add(id, SPR.TRANSFORM, vf.createLiteral(expr, XMLSchema.STRING));
        }

        return id;
    }

    // ID must be assigned in order for the method to work

    public void parseRDF(final Graph graph, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces) throws MalformedQueryException
    {
        checkMutable();
        Preconditions.checkNotNull(graph);
        Preconditions.checkState(this.id != null);

        final Selector s = Selector.select(graph, this.id);
        if (!s.isEmpty(SPR.TRIGGER_OF)) {
            this.triggeredRuleIDs = Sets.newHashSet(s.getList(SPR.TRIGGER_OF, Resource.class));
        }

        String field = null;
        String expr = null;
        try {
            field = "head";
            expr = s.get(SPR.HEAD, String.class, null);
            if (expr != null) {
                this.head = Algebra.parseTupleExpr(expr, baseURI, namespaces);
            }

            field = "body";
            expr = s.get(SPR.BODY, String.class, null);
            if (expr != null) {
                this.body = Algebra.parseTupleExpr(expr, baseURI, namespaces);
            }

            field = "condition";
            expr = s.get(SPR.CONDITION, String.class, null);
            if (expr != null) {
                this.condition = Algebra.parseValueExpr(expr, baseURI, namespaces);
            }

            field = "transform";
            expr = s.get(SPR.TRANSFORM, String.class, null);
            if (expr != null) {
                this.transform = (FunctionCall) Algebra.parseValueExpr(expr, baseURI, namespaces);
            }

        } catch (final MalformedQueryException ex) {
            throw new IllegalArgumentException("Invalid rule " + this.id + ": malformed " + field
                    + " - " + ex.getMessage() + "\n" + expr, ex);
        }
    }

    public static Rule parseRDF(final Graph graph, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces, final Resource ruleID)
            throws MalformedQueryException
    {
        final Resource id = ruleID != null ? ruleID : Iterators.getOnlyElement(
                graph.match(null, RDF.TYPE, SPR.RULE)).getSubject();
        final Rule rule = new Rule();
        rule.setID(id);
        rule.parseRDF(graph, baseURI, namespaces);
        return rule;
    }

    // VALIDATION

    public void validate()
    {
        validate(this.id != null, "no ID defined");
        validate(this.head != null, "no head supplied");
        validate(this.body != null, "no body supplied");
        validate(this.triggeredRuleIDs == null || !this.triggeredRuleIDs.contains(null),
                "invalid list of triggered rules IDs");
    }

    private void validate(final boolean condition, final String message)
    {
        if (!condition) {
            throw new IllegalStateException("Invalid rule"
                    + (this.id == null ? "" : " " + this.id.stringValue()) + ": " + message);
        }
    }

    // FREEZING AND CLONING

    public boolean isFrozen()
    {
        return this.frozen;
    }

    public void freeze()
    {
        if (!this.frozen) {
            this.triggeredRuleIDs = this.triggeredRuleIDs == null ? null : ImmutableSet
                    .copyOf(this.triggeredRuleIDs);
            this.frozen = true;
        }
    }

    @Override
    public Rule clone()
    {
        try {
            final Rule clone = (Rule) super.clone();
            clone.triggeredRuleIDs = this.triggeredRuleIDs == null ? null : Sets
                    .newHashSet(this.triggeredRuleIDs);
            clone.frozen = false;
            return clone;

        } catch (final CloneNotSupportedException ex) {
            throw new Error("Unexpected exception: " + ex.getMessage(), ex);
        }
    }

    private void checkMutable()
    {
        if (this.frozen) {
            throw new IllegalStateException("Cannot modify frozen rule " + this.id
                    + "; must clone it");
        }
    }

    // COMPARISON AND HASHING

    @Override
    public boolean equals(@Nullable final Object object)
    {
        if (object == this) {
            return true;
        }
        if (object == null || !(object instanceof Rule)) {
            return false;
        }
        final Rule other = (Rule) object;
        return Objects.equal(this.id, other.id) && Objects.equal(this.head, other.head)
                && Objects.equal(this.body, other.body)
                && Objects.equal(this.condition, other.condition)
                && Objects.equal(this.transform, other.transform)
                && Objects.equal(this.triggeredRuleIDs, other.triggeredRuleIDs);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(this.id, this.head, this.body, this.condition, this.transform,
                this.triggeredRuleIDs);
    }

    public String digest()
    {
        String digest = this.digest;
        if (digest == null) {
            final Hasher hasher = Hashing.md5().newHasher();
            hasher.putUnencodedChars(this.id == null ? " " : this.id.stringValue());
            hasher.putUnencodedChars(this.head == null ? " " : SparqlRenderer.render(this.head).toString());
            hasher.putUnencodedChars(this.body == null ? " " : SparqlRenderer.render(this.body).toString());
            hasher.putUnencodedChars(this.condition == null ? " " : SparqlRenderer.render(this.condition)
                    .toString());
            hasher.putUnencodedChars(this.transform == null ? " " : SparqlRenderer.render(this.transform)
                    .toString());
            final List<String> ids = Lists.newArrayList();
            if (this.triggeredRuleIDs != null) {
                for (final Resource ruleID : this.triggeredRuleIDs) {
                    ids.add(ruleID.stringValue());
                }
            }
            Collections.sort(ids);
            hasher.putUnencodedChars(Joiner.on(" ").join(ids));
            digest = hasher.hash().toString();
            this.digest = this.frozen ? digest : null;
        }
        return digest;
    }

    // STRING REPRESENTATION

    @Override
    public String toString()
    {
        return this.id instanceof URI ? ((URI) this.id).getLocalName() : "unnamed";
    }

    // HANDLER INTERFACE

    public interface StatementHandler<E extends Exception>
    {

        void handle(Resource subj, URI pred, Value obj, Resource ctx) throws E;

    }

}
