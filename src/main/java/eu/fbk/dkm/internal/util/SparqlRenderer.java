package eu.fbk.dkm.internal.util;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.FN;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.Add;
import org.eclipse.rdf4j.query.algebra.AggregateOperator;
import org.eclipse.rdf4j.query.algebra.And;
import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.Avg;
import org.eclipse.rdf4j.query.algebra.BNodeGenerator;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.BinaryValueOperator;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Bound;
import org.eclipse.rdf4j.query.algebra.Clear;
import org.eclipse.rdf4j.query.algebra.Coalesce;
import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.Compare.CompareOp;
import org.eclipse.rdf4j.query.algebra.Copy;
import org.eclipse.rdf4j.query.algebra.Count;
import org.eclipse.rdf4j.query.algebra.Create;
import org.eclipse.rdf4j.query.algebra.Datatype;
import org.eclipse.rdf4j.query.algebra.DeleteData;
import org.eclipse.rdf4j.query.algebra.Difference;
import org.eclipse.rdf4j.query.algebra.Distinct;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.Exists;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.Group;
import org.eclipse.rdf4j.query.algebra.GroupConcat;
import org.eclipse.rdf4j.query.algebra.IRIFunction;
import org.eclipse.rdf4j.query.algebra.If;
import org.eclipse.rdf4j.query.algebra.InsertData;
import org.eclipse.rdf4j.query.algebra.IsBNode;
import org.eclipse.rdf4j.query.algebra.IsLiteral;
import org.eclipse.rdf4j.query.algebra.IsNumeric;
import org.eclipse.rdf4j.query.algebra.IsURI;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Lang;
import org.eclipse.rdf4j.query.algebra.LangMatches;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.Load;
import org.eclipse.rdf4j.query.algebra.MathExpr;
import org.eclipse.rdf4j.query.algebra.Max;
import org.eclipse.rdf4j.query.algebra.Min;
import org.eclipse.rdf4j.query.algebra.Modify;
import org.eclipse.rdf4j.query.algebra.Move;
import org.eclipse.rdf4j.query.algebra.MultiProjection;
import org.eclipse.rdf4j.query.algebra.Not;
import org.eclipse.rdf4j.query.algebra.Or;
import org.eclipse.rdf4j.query.algebra.Order;
import org.eclipse.rdf4j.query.algebra.OrderElem;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.Reduced;
import org.eclipse.rdf4j.query.algebra.Regex;
import org.eclipse.rdf4j.query.algebra.SameTerm;
import org.eclipse.rdf4j.query.algebra.Sample;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.Slice;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.StatementPattern.Scope;
import org.eclipse.rdf4j.query.algebra.Str;
import org.eclipse.rdf4j.query.algebra.Sum;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.UnaryValueOperator;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import eu.fbk.dkm.internal.util.SparqlQuery.QueryForm;

public final class SparqlRenderer
{

    private static final String DEFAULT_INDENT_STRING = "  ";

    private static final String DEFAULT_LINE_PREFIX = "";

    private static final Map<Class<?>, String> UPDATE_OPERATOR_NAMES;

    private static final Map<CompareOp, String> COMPARE_OPERATOR_NAMES;

    private static final Map<Class<?>, String> VALUE_OPERATOR_NAMES;

    private static final Map<Class<?>, Integer> VALUE_OPERATOR_PRIORITIES;

    static {
        UPDATE_OPERATOR_NAMES = ImmutableMap.<Class<?>, String>builder()
                .put(InsertData.class, "INSERT DATA").put(DeleteData.class, "DELETE DATA")
                .put(Add.class, "ADD").put(Clear.class, "CLEAR").put(Copy.class, "COPY")
                .put(Load.class, "LOAD").put(Move.class, "MOVE").put(Create.class, "CREATE")
                .build();

        COMPARE_OPERATOR_NAMES = ImmutableMap.<CompareOp, String>builder().put(CompareOp.EQ, "=")
                .put(CompareOp.GE, ">=").put(CompareOp.GT, ">").put(CompareOp.LE, "<=")
                .put(CompareOp.LT, "<").put(CompareOp.NE, "!=").build();

        VALUE_OPERATOR_NAMES = ImmutableMap.<Class<?>, String>builder().put(Avg.class, "AVG")
                .put(Count.class, "COUNT").put(GroupConcat.class, "GROUP_CONCAT")
                .put(Max.class, "MAX").put(Min.class, "MIN").put(Sample.class, "SAMPLE")
                .put(Sum.class, "SUM").put(IRIFunction.class, "IRI").put(IsBNode.class, "isBlank")
                .put(IsLiteral.class, "isLiteral").put(IsNumeric.class, "isNumeric")
                .put(IsURI.class, "isIRI").put(Lang.class, "lang").put(Not.class, "!")
                .put(Str.class, "str").put(Or.class, "||").put(And.class, "&&")
                .put(LangMatches.class, "langMatches").put(Regex.class, "regex")
                .put(SameTerm.class, "sameTerm").put(Bound.class, "bound")
                .put(Coalesce.class, "COALESCE").put(If.class, "IF")
                .put(Datatype.class, "DATATYPE").build();

        VALUE_OPERATOR_PRIORITIES = ImmutableMap.<Class<?>, Integer>builder().put(Not.class, 4)
                .put(And.class, 3).put(Or.class, 2).put(MathExpr.class, 1).put(Compare.class, 0)
                .build();
    }

    // referenced twice or more

    /**
     * @uml.property name="nodes"
     */
    private final Iterable<? extends QueryModelNode> nodes;

    /**
     * @uml.property name="datasets"
     * @uml.associationEnd multiplicity="(0 -1)" elementType="org.openrdf.query.Dataset"
     */
    private final Iterable<? extends Dataset> datasets;

    /**
     * @uml.property name="referencedNamespaces"
     * @uml.associationEnd multiplicity="(0 -1)" elementType="java.lang.String"
     */
    private Set<String> referencedNamespaces;

    /**
     * @uml.property name="baseURI"
     */
    private String baseURI;

    /**
     * @uml.property name="prefixes"
     * @uml.associationEnd qualifier="getNamespace:java.lang.String java.lang.String"
     */
    private Map<String, String> prefixes;

    /**
     * @uml.property name="indentLevel"
     */
    private int indentLevel;

    /**
     * @uml.property name="indentString"
     */
    private String indentString;

    /**
     * @uml.property name="linePrefix"
     */
    private String linePrefix;

    /**
     * @uml.property name="headerRendered"
     */
    private boolean headerRendered;

    /**
     * @uml.property name="currentDataset"
     * @uml.associationEnd
     */
    private Dataset currentDataset;

    /**
     * @uml.property name="variableRefCount"
     * @uml.associationEnd qualifier="getName:java.lang.String java.lang.Integer"
     */
    private Map<String, Integer> variableRefCount;

    /**
     * @uml.property name="lastPattern"
     * @uml.associationEnd
     */
    private StatementPattern lastPattern;

    /**
     * @uml.property name="builder"
     */
    private StringBuilder builder;

    private SparqlRenderer(final Iterable<? extends QueryModelNode> nodes,
            final Iterable<? extends Dataset> datasets, final boolean headerRendered)
    {
        this.nodes = nodes;
        this.datasets = datasets;
        this.referencedNamespaces = null;
        this.baseURI = null;
        this.prefixes = Collections.emptyMap();
        this.indentLevel = 0;
        this.indentString = SparqlRenderer.DEFAULT_INDENT_STRING;
        this.linePrefix = SparqlRenderer.DEFAULT_LINE_PREFIX;
        this.headerRendered = headerRendered;
        this.currentDataset = null;
        this.variableRefCount = null;
        this.lastPattern = null;
        this.builder = null;
    }

    // PUBLIC API

    public static SparqlRenderer render(final Value value)
    {
        Preconditions.checkNotNull(value);
        return new SparqlRenderer(Collections.singleton(new ValueConstant(value)),
                Collections.<Dataset>singleton(null), false);
    }

    public static SparqlRenderer render(final QueryModelNode node)
    {
        Preconditions.checkNotNull(node);
        return new SparqlRenderer(Collections.singleton(node),
                Collections.<Dataset>singleton(null), false);
    }

    public static SparqlRenderer render(final TupleExpr query, @Nullable final Dataset dataset)
    {
        Preconditions.checkNotNull(query);
        return new SparqlRenderer(Collections.singleton(query), Collections.singleton(dataset),
                true);
    }

    public static SparqlRenderer render(final UpdateExpr update, @Nullable final Dataset dataset)
    {
        Preconditions.checkNotNull(update);
        return new SparqlRenderer(Collections.singleton(update), Collections.singleton(dataset),
                true);
    }

    public static SparqlRenderer render(final Iterable<? extends UpdateExpr> updates,
            final Iterable<? extends Dataset> datasets)
    {
        Preconditions.checkNotNull(updates);
        Preconditions.checkNotNull(datasets);
        return new SparqlRenderer(updates, datasets, true);
    }

    public SparqlRenderer withBaseURI(final String baseURI)
    {
        this.baseURI = baseURI;
        return this;
    }

    public SparqlRenderer withPrefixes(final Map<String, String> prefixes)
    {
        Preconditions.checkNotNull(prefixes);
        this.prefixes = prefixes;
        return this;
    }

    public SparqlRenderer withNamespaces(final Map<String, String> namespaces)
    {
        Preconditions.checkNotNull(namespaces);
        this.prefixes = Maps.newLinkedHashMap();
        for (final Map.Entry<String, String> entry : namespaces.entrySet()) {
            this.prefixes.put(entry.getValue(), entry.getKey());
        }
        return this;
    }

    public SparqlRenderer withIndentString(final String indentString)
    {
        Preconditions.checkNotNull(indentString);
        this.indentString = indentString;
        return this;
    }

    public SparqlRenderer withLinePrefix(final String linePrefix)
    {
        Preconditions.checkNotNull(linePrefix);
        this.linePrefix = linePrefix;
        return this;
    }

    public SparqlRenderer withHeader(final boolean headerRendered)
    {
        this.headerRendered = headerRendered;
        return this;
    }

    public void toStringBuilder(final StringBuilder builder)
    {
        if (this.headerRendered) {
            this.referencedNamespaces = Sets.newHashSet();
            this.builder = new StringBuilder();
        } else {
            this.builder = builder;
            this.emit(this.linePrefix);
        }

        final Iterator<? extends Dataset> iterator = this.datasets.iterator();
        boolean first = true;
        for (final QueryModelNode node : this.nodes) {
            if (!first) {
                this.emit(";").newline().newline();
            }
            first = false;
            this.currentDataset = iterator.next();
            this.variableRefCount = SparqlRenderer.collectVariableRefCount(node);
            this.emitNode(node);
        }

        if (this.headerRendered) {
            final String body = this.builder.toString();
            this.builder = builder;
            this.emit(this.linePrefix).emitHeader();
            this.builder.append(body);
        }
    }

    @Override
    public String toString()
    {
        final StringBuilder builder = new StringBuilder();
        this.toStringBuilder(builder);
        return builder.toString();
    }

    private static Map<String, Integer> collectVariableRefCount(final QueryModelNode rootNode)
    {
        final Map<String, Integer> map = Maps.newHashMap();
        rootNode.visit(new AbstractQueryModelVisitor<RuntimeException>() {

            @Override
            public void meet(final Var var)
            {
                final Integer oldCount = map.get(var.getName());
                map.put(var.getName(), oldCount == null ? 1 : oldCount + 1);
            }

        });
        return map;
    }

    // BASIC RENDERING METHODS (STRING, VALUES, CONDITIONALS, NEWLINE AND BRACES, ERRORS)

    private SparqlRenderer emit(final String string)
    {
        this.builder.append(string);
        return this;
    }

    private SparqlRenderer emitIf(final boolean condition, final String string)
    {
        if (condition) {
            this.builder.append(string);
        }
        return this;
    }

    private SparqlRenderer emitEscaped(final String label)
    {
        final int length = label.length();
        for (int i = 0; i < length; ++i) {
            final char c = label.charAt(i);
            if (c == '\\') {
                this.emit("\\\\");
            } else if (c == '"') {
                this.emit("\\\"");
            } else if (c == '\n') {
                this.emit("\\n");
            } else if (c == '\r') {
                this.emit("\\r");
            } else if (c == '\t') {
                this.emit("\\t");
            } else if (c >= 0x0 && c <= 0x8 || c == 0xB || c == 0xC || c >= 0xE && c <= 0x1F
                    || c >= 0x7F && c <= 0xFFFF) {
                this.emit("\\u")
                        .emit(Strings.padStart(Integer.toHexString(c).toUpperCase(), 4, '0'));
            } else if (c >= 0x10000 && c <= 0x10FFFF) {
                this.emit("\\U")
                        .emit(Strings.padStart(Integer.toHexString(c).toUpperCase(), 8, '0'));
            } else {
                this.emit(Character.toString(c));
            }
        }
        return this;
    }

    private SparqlRenderer emitValue(final Value value)
    {
        if (value instanceof BNode) {
            return this.emit("_:").emit(((BNode) value).getID());

        } else if (value instanceof IRI) {
            final IRI uri = (IRI) value;
            final String prefix = this.prefixes.get(uri.getNamespace());
            if (prefix != null) {
                if (this.referencedNamespaces != null) {
                    this.referencedNamespaces.add(uri.getNamespace());
                }
                return this.emit(prefix).emit(":").emit(uri.getLocalName());
            } else if (this.baseURI != null && uri.toString().startsWith(this.baseURI)) {
                return this.emit("<").emitEscaped(uri.toString().substring(this.baseURI.length()))
                        .emit(">");
            } else {
                return this.emit("<").emitEscaped(uri.toString()).emit(">");
            }

        } else if (value instanceof Literal) {
            final Literal literal = (Literal) value;
            if (XMLSchema.INTEGER.equals(literal.getDatatype())) {
                this.emit(Integer.parseInt(
                        literal.getLabel().startsWith("+") ? literal.getLabel().substring(1)
                                : literal.getLabel())
                        + "");
            } else {
                this.emit("\"").emit(literal.getLabel()).emit("\"");
                if (literal.getDatatype() != null) {
                    this.emit("^^").emitValue(literal.getDatatype());
                } else if (literal.getLanguage().isPresent()) {
                    this.emit("@").emit(literal.getLanguage().get());
                }
            }
            return this;

        } else {
            throw new Error("Unexpected value: " + value);
        }
    }

    private SparqlRenderer newline()
    {
        this.emit("\n");
        this.emit(this.linePrefix);
        for (int i = 0; i < this.indentLevel; ++i) {
            this.emit(this.indentString);
        }
        return this;
    }

    private SparqlRenderer openBrace()
    {
        this.emit("{");
        ++this.indentLevel;
        this.newline();
        return this;
    }

    private SparqlRenderer closeBrace()
    {
        --this.indentLevel;
        this.newline();
        this.emit("}");
        return this;
    }

    private SparqlRenderer renderingFailed(final String message, final QueryModelNode node)
    {
        throw new IllegalArgumentException("SPARQL rendering failed. " + message
                + (node == null ? "null" : node.getClass().getSimpleName() + "\n" + node));
    }

    // RENDERING OF HEADER

    private SparqlRenderer emitHeader()
    {
        if (!this.referencedNamespaces.isEmpty()) {
            for (final String namespace : Ordering.natural() //
                    .sortedCopy(this.referencedNamespaces)) {
                this.emit("PREFIX ").emit(this.prefixes.get(namespace)).emit(": <")
                        .emitEscaped(namespace).emit(">").newline();
            }
            if (this.baseURI != null) {
                this.emit("BASE <").emitEscaped(this.baseURI).emit(">").newline();
            }
            this.newline();
        }
        return this;
    }

    // RENDERING OF NODES

    private SparqlRenderer emitNode(final QueryModelNode node)
    {
        if (node instanceof UpdateExpr) {
            this.emitUpdateExpr((UpdateExpr) node);
        } else if (node instanceof TupleExpr) {
            this.emitTupleExpr((TupleExpr) node);
        } else if (node instanceof ValueExpr) {
            this.emitValueExpr((ValueExpr) node);
        }
        return this;
    }

    // RENDERING OF UPDATE EXPRESSIONS

    private SparqlRenderer emitUpdateExpr(final UpdateExpr node)
    {
        Preconditions.checkArgument(
                !node.isSilent() || !(node instanceof InsertData) && !(node instanceof DeleteData)
                        && !(node instanceof Modify),
                "SILENT unsupported for INSERT DATA / DELETE DATA / MODIFY");

        if (!(node instanceof Modify)) {
            this.emitDatasetAsComment(true, true)
                    .emit(SparqlRenderer.UPDATE_OPERATOR_NAMES.get(node.getClass())).emit(" ")
                    .emitIf(node.isSilent(), "SILENT ");
        }

        if (node instanceof InsertData) {
            if (node instanceof Reduced && ((Reduced) node) //
                    .getArg() instanceof EmptySet) {
                this.openBrace().closeBrace();
            } else {
                this.openBrace().emitTupleExpr(
                        SparqlQuery.fromTupleExpr((TupleExpr) node, null).getConstructExpression())
                        .closeBrace();
            }

        } else if (node instanceof DeleteData) {
            if (node instanceof Reduced && ((Reduced) node) //
                    .getArg() instanceof EmptySet) {
                this.openBrace().closeBrace();
            } else {
                this.openBrace().emitTupleExpr(
                        SparqlQuery.fromTupleExpr((TupleExpr) node, null).getConstructExpression())
                        .closeBrace();
            }

        } else if (node instanceof Modify) {
            final Modify modify = (Modify) node;
            if (modify.getDeleteExpr() != null) {
                this.emit("DELETE ").openBrace().emitTupleExpr(modify.getDeleteExpr()).closeBrace()
                        .newline();
            }
            if (modify.getInsertExpr() != null) {
                this.emit("INSERT ").openBrace().emitTupleExpr(modify.getInsertExpr()).closeBrace()
                        .newline();
            }
            if (this.currentDataset != null) {
                for (final IRI uri : this.currentDataset.getDefaultGraphs()) {
                    this.emit("USING ").emitNode(new ValueConstant(uri)).newline();
                }
                for (final IRI uri : this.currentDataset.getNamedGraphs()) {
                    this.emit("USING NAMED ").emitNode(new ValueConstant(uri)).newline();
                }
                this.emitDatasetAsComment(false, true);
            }
            this.emit("WHERE ").openBrace().emitTupleExpr(modify.getWhereExpr()).closeBrace();

        } else if (node instanceof Add) {
            final Add add = (Add) node;
            this.emitUpdateArg(add.getSourceGraph(), Scope.DEFAULT_CONTEXTS).newline();
            this.emit("TO ").emitUpdateArg(add.getDestinationGraph(), Scope.DEFAULT_CONTEXTS);

        } else if (node instanceof Clear) {
            this.emitUpdateArg(((Clear) node).getGraph(), ((Clear) node).getScope());

        } else if (node instanceof Copy) {
            final Copy copy = (Copy) node;
            this.emitUpdateArg(copy.getSourceGraph(), Scope.DEFAULT_CONTEXTS).newline();
            this.emit("TO ").emitUpdateArg(copy.getDestinationGraph(), Scope.DEFAULT_CONTEXTS);

        } else if (node instanceof Create) {
            this.emit("GRAPH ").emitNode(((Create) node).getGraph());

        } else if (node instanceof Load) {
            final Load load = (Load) node;
            this.emitValueExpr(load.getSource());
            if (load.getGraph() != null) {
                this.newline().emit("INTO GRAPH ").emitValueExpr(load.getGraph());
            }

        } else if (node instanceof Move) {
            final Move move = (Move) node;
            this.emitUpdateArg(move.getSourceGraph(), Scope.DEFAULT_CONTEXTS).newline();
            this.emit("TO ").emitUpdateArg(move.getDestinationGraph(), Scope.DEFAULT_CONTEXTS);
        }

        return this;
    }

    private SparqlRenderer emitDatasetAsComment(final boolean readPart, final boolean writePart)
    {
        if (this.currentDataset != null) {
            if (readPart) {
                for (final IRI uri : this.currentDataset.getDefaultGraphs()) {
                    this.emit("# FROM ").emitValue(uri).newline();
                }
                for (final IRI uri : this.currentDataset.getNamedGraphs()) {
                    this.emit("# FROM NAMED ").emitValue(uri).newline();
                }
            }
            if (writePart) {
                if (this.currentDataset.getDefaultInsertGraph() != null) {
                    this.emit("# INSERT INTO ")
                            .emitValue(this.currentDataset.getDefaultInsertGraph()).newline();
                }
                for (final IRI uri : this.currentDataset.getDefaultRemoveGraphs()) {
                    this.emit("# REMOVE FROM ").emitValue(uri).newline();
                }
            }
        }
        return this;
    }

    private SparqlRenderer emitUpdateArg(final ValueExpr graphExpr,
            final StatementPattern.Scope scopeIfGraphUnspecified)
    {
        if (graphExpr != null) {
            this.emit("GRAPH ").emitValueExpr(graphExpr);
        } else if (scopeIfGraphUnspecified == StatementPattern.Scope.DEFAULT_CONTEXTS) {
            this.emit("DEFAULT");
        } else if (scopeIfGraphUnspecified == StatementPattern.Scope.NAMED_CONTEXTS) {
            this.emit("NAMED");
        } else {
            this.emit("ALL");
        }
        return this;
    }

    // RENDERING OF TUPLE EXPRESSIONS

    private SparqlRenderer emitTupleExpr(final TupleExpr node)
    {
        if (node instanceof QueryRoot) {
            this.emitTupleExpr(((QueryRoot) node).getArg());

        } else if (node instanceof Projection || node instanceof MultiProjection
                || node instanceof Distinct || node instanceof Reduced || node instanceof Slice
                || node instanceof Order || node instanceof Group) {
            final SparqlQuery query = SparqlQuery.fromTupleExpr(node, this.currentDataset);
            if (node.getParentNode() != null) {
                this.openBrace().emitQuery(query).closeBrace();
            } else {
                this.emitQuery(query);
            }

        } else if (node instanceof UnaryTupleOperator) {
            this.emitUnaryTupleOperator((UnaryTupleOperator) node);

        } else if (node instanceof BinaryTupleOperator) {
            this.emitBinaryTupleOperator((BinaryTupleOperator) node);

        } else if (node instanceof StatementPattern) {
            this.emitStatementPattern((StatementPattern) node);

        } else if (node instanceof SingletonSet) {
            // nothing

        } else if (node instanceof EmptySet) {
            Preconditions.checkArgument(
                    node.getParentNode() == null || node.getParentNode() instanceof QueryRoot,
                    "Cannot translate EmptySet inside the body of a query / update operation");
            this.emit("CONSTRUCT {} WHERE {}");

        } else if (node instanceof ArbitraryLengthPath) {
            final ArbitraryLengthPath path = (ArbitraryLengthPath) node;
            this.emitPropertyPath(path, path.getSubjectVar(), path.getObjectVar());

        } else if (node instanceof BindingSetAssignment) {
            this.emitBindingSetAssignment((BindingSetAssignment) node);

        } else {
            this.renderingFailed("Unsupported3 node: ", node);
        }
        return this;
    }

    // RENDERING OF TUPLE EXPRESSIONS - QUERIES

    private SparqlRenderer emitQuery(final SparqlQuery query)
    {
        if (query.getQueryForm() == QueryForm.ASK) {
            this.emit("ASK");
        } else if (query.getQueryForm() == QueryForm.SELECT) {
            this.emit("SELECT ");
            if (query.getSelectModifier() != null) {
                this.emit(query.getSelectModifier().toString()).emit(" ");
            }
            this.emitProjectionElems(query.getSelectList());
        } else if (query.getQueryForm() == QueryForm.CONSTRUCT) {
            this.emit("CONSTRUCT ").openBrace().emitTupleExpr(query.getConstructExpression())
                    .closeBrace();
        }

        if (query.getFromDataset() != null) {
            for (final IRI uri : query.getFromDataset().getDefaultGraphs()) {
                this.newline().emit("FROM ").emitValue(uri);
            }
            for (final IRI uri : query.getFromDataset().getNamedGraphs()) {
                this.newline().emit("FROM NAMED ").emitValue(uri);
            }
        }

        this.newline().emit("WHERE ").openBrace().emitTupleExpr(query.getWhereExpression())
                .closeBrace();

        if (!query.getGroupByList().isEmpty()) {
            this.newline().emit("GROUP BY ").emitProjectionElems(query.getGroupByList());
        }

        if (query.getHavingCondition() != null) {
            this.newline().emit("HAVING (").emitValueExpr(query.getHavingCondition()).emit(")");
        }

        if (!query.getOrderByList().isEmpty()) {
            this.newline().emit("ORDER BY ").emitOrderElems(query.getOrderByList());
        }

        if (query.getQueryForm() != QueryForm.ASK) {
            if (query.getSliceOffset() != null) {
                this.newline().emit("OFFSET " + query.getSliceOffset());
            }
            if (query.getSliceLimit() != null) {
                this.newline().emit("LIMIT " + query.getSliceLimit());
            }
        }

        return this;
    }

    private SparqlRenderer emitProjectionElems(final Iterable<? extends ProjectionElem> elems)
    {
        boolean first = true;
        for (final ProjectionElem elem : elems) {
            this.emitIf(!first, " ");
            if (elem.getSourceExpression() != null) {
                this.emit("(").emitValueExpr(elem.getSourceExpression().getExpr()).emit(" AS ?")
                        .emit(elem.getTargetName()).emit(")");
            } else {
                this.emit("?").emit(elem.getTargetName());
            }
            first = false;
        }
        return this;
    }

    private SparqlRenderer emitOrderElems(final Iterable<? extends OrderElem> elems)
    {
        boolean first = true;
        for (final OrderElem orderElem : elems) {
            this.emitIf(!first, " ").emit(orderElem.isAscending() ? "asc(" : "desc(")
                    .emitValueExpr(orderElem.getExpr()).emit(")");
            first = false;
        }
        return this;
    }

    // RENDERING OF TUPLE EXPRESSIONS - STATEMENT PATTERNS

    private SparqlRenderer emitStatementPattern(final StatementPattern node)
    {
        final boolean insideGraph = Scope.NAMED_CONTEXTS.equals(node.getScope());
        if (this.lastPattern == null && insideGraph) {
            this.emit("GRAPH ").emitValueExpr(node.getContextVar()).emit(" ").openBrace();
        }

        if (this.lastPattern == null) {
            // s p o
            this.emitValueExpr(node.getSubjectVar()).emit(" ")
                    .emitValueExpr(node.getPredicateVar()).emit(" ")
                    .emitValueExpr(node.getObjectVar());
        } else if (!this.lastPattern.getSubjectVar().equals(node.getSubjectVar())) {
            // s1 p1 o1.\n s2 p2 o2
            this.emit(".").newline().emitValueExpr(node.getSubjectVar()).emit(" ")
                    .emitValueExpr(node.getPredicateVar()).emit(" ")
                    .emitValueExpr(node.getObjectVar());
        } else if (!this.lastPattern.getPredicateVar().equals(node.getPredicateVar())) {
            // s p1 o1;\n p2 o2
            this.emit(";").newline().emitValueExpr(node.getPredicateVar()).emit(" ")
                    .emitValueExpr(node.getObjectVar());
        } else if (!this.lastPattern.getObjectVar().equals(node.getObjectVar())) {
            // s p o1, o2
            this.emit(", ").emitValueExpr(node.getObjectVar());
        }

        this.lastPattern = null;
        if (!this.isLastPatternOfGraph(node)) {
            this.lastPattern = node;
        } else {
            if (insideGraph) {
                this.closeBrace();
            }
            if (!this.isLastPattern(node)) {
                this.newline();
            }
        }
        return this;
    }

    private boolean isLastPatternOfGraph(final StatementPattern pattern)
    {
        QueryModelNode node = pattern;
        while (node.getParentNode() instanceof Join
                && ((Join) node.getParentNode()).getRightArg() == node) {
            node = node.getParentNode();
        }
        node = node.getParentNode();
        if (!(node instanceof Join)) {
            return true;
        }
        node = ((Join) node).getRightArg();
        while (node instanceof Join) {
            node = ((Join) node).getLeftArg();
        }
        return !(node instanceof StatementPattern) || !Objects
                .equal(((StatementPattern) node).getContextVar(), pattern.getContextVar());
    }

    private boolean isLastPattern(final StatementPattern pattern)
    {
        QueryModelNode node = pattern;
        while (node.getParentNode() != null) {
            if (!(node.getParentNode() instanceof Join)) {
                return true;
            } else {
                final Join join = (Join) node.getParentNode();
                if (join.getLeftArg() == node && (join.getRightArg() instanceof Join || //
                        join.getRightArg() instanceof StatementPattern)) {
                    return false;
                }
            }
            node = node.getParentNode();
        }
        return true;
    }

    // RENDERING OF TUPLE EXPRESSIONS - UNARY AND BINARY OPERATORS

    private SparqlRenderer emitUnaryTupleOperator(final UnaryTupleOperator node)
    {
        if (node instanceof Filter) {
            // XXX handle property paths encoded using filter (optional feature)
            final Filter filter = (Filter) node;
            this.emitTupleExpr(filter.getArg()).newline().emit("FILTER");
            if (filter.getCondition() instanceof Exists || filter.getCondition() instanceof Not
                    && ((Not) filter.getCondition()).getArg() instanceof Exists) {
                this.emit(" ").emitNode(filter.getCondition());
            } else {
                this.emit(" (").emitValueExpr(filter.getCondition()).emit(")");
            }

        } else if (node instanceof Extension) {
            this.emitTupleExpr(node.getArg());
            for (final ExtensionElem elem : ((Extension) node).getElements()) {
                if (!(elem.getExpr() instanceof Var)
                        || !((Var) elem.getExpr()).getName().equals(elem.getName())) {
                    this.newline().emit("BIND (").emitValueExpr(elem.getExpr()).emit(" AS ?")
                            .emit(elem.getName()).emit(")");
                }
            }

        } else if (node instanceof Service) {
            final Service service = (Service) node;
            this.newline().emit("SERVICE ").emitIf(service.isSilent(), "SILENT ").openBrace()
                    .emitTupleExpr(service.getServiceExpr()).closeBrace().emit(" ")
                    .emitValueExpr(service.getServiceRef());

        } else {
            this.renderingFailed("Unsupported4 node: ", node);
        }
        return this;
    }

    private SparqlRenderer emitBinaryTupleOperator(final BinaryTupleOperator node)
    {
        if (node instanceof Join) {
            this.emitTupleExpr(node.getLeftArg()).emitTupleExpr(node.getRightArg());

        } else if (node instanceof LeftJoin) {
            final LeftJoin join = (LeftJoin) node;
            this.emitTupleExpr(join.getLeftArg()).newline().emit("OPTIONAL ").openBrace()
                    .emitTupleExpr(join.getRightArg());
            if (join.getCondition() != null) {
                this.newline().emit("FILTER");
                if (join.getCondition() instanceof Exists || join.getCondition() instanceof Not
                        && ((Not) join.getCondition()).getArg() instanceof Exists) {
                    this.emit(" ").emitNode(join.getCondition());
                } else {
                    this.emit(" (").emitValueExpr(join.getCondition()).emit(")");
                }
            }
            this.closeBrace();

        } else if (node instanceof Difference) {
            this.openBrace().emitTupleExpr(node.getLeftArg()).closeBrace().emit(" MINUS ")
                    .openBrace().emitTupleExpr(node.getRightArg()).closeBrace();

        } else if (node instanceof Union) {
            ZeroLengthPath path = null;
            if (node.getLeftArg() instanceof ZeroLengthPath) {
                path = (ZeroLengthPath) node.getLeftArg();
            } else if (node.getRightArg() instanceof ZeroLengthPath) {
                path = (ZeroLengthPath) node.getRightArg();
            }
            if (path == null) {
                this.openBrace().emitTupleExpr(node.getLeftArg()).closeBrace().emit(" UNION ")
                        .openBrace().emitTupleExpr(node.getRightArg()).closeBrace();
            } else {
                if (path.getContextVar() != null) {
                    this.emit("GRAPH ").emitValueExpr(path.getContextVar()).emit(" ").openBrace();
                }
                this.emitValueExpr(path.getSubjectVar()).emit(" ")
                        .emitPropertyPath(node, path.getSubjectVar(), path.getObjectVar())
                        .emit(" ").emitValueExpr(path.getObjectVar());
                if (path.getContextVar() != null) {
                    this.closeBrace();
                }
            }
        }
        return this;
    }

    private SparqlRenderer emitBindingSetAssignment(final BindingSetAssignment node)
    {
        if (node.getBindingNames().size() == 0) {
            return this.newline().emit("VALUES {}");

        } else if (node.getBindingNames().size() == 1) {
            final String name = Iterables.getOnlyElement(node.getBindingNames());
            this.newline().emit("VALUES ?").emit(name).emit(" ").openBrace();
            boolean first = true;
            for (final BindingSet bindingSet : node.getBindingSets()) {
                this.emitIf(!first, " ").emitValue(bindingSet.getValue(name));
                first = false;
            }
            return this.closeBrace();

        } else {
            this.newline().emit("VALUES (");
            boolean first = true;
            for (final String name : node.getBindingNames()) {
                this.emitIf(!first, " ").emit("?").emit(name);
                first = false;
            }
            this.emit(") ").openBrace();
            for (final BindingSet bindingSet : node.getBindingSets()) {
                this.emit("(");
                first = true;
                for (final String name : node.getBindingNames()) {
                    this.emitIf(!first, " ").emitValue(bindingSet.getValue(name));
                    first = false;
                }
                this.emit(")").newline();
            }
            return this.closeBrace();
        }
    }

    // RENDERING OF TUPLE EXPRESSIONS - PROPERTY PATHS

    private SparqlRenderer emitPropertyPath(final TupleExpr node, final Var start, final Var end)
    {
        // Note: elt1 / elt2 and ^(complex exp) do not occur in Sesame algebra

        final boolean parenthesis = node instanceof Union && node.getParentNode() != null
                && node.getParentNode() instanceof ArbitraryLengthPath;

        this.emitIf(parenthesis, "(");

        if (node instanceof StatementPattern) {

            // handles iri, ^iri
            final StatementPattern pattern = (StatementPattern) node;
            final boolean inverse = this.isInversePath(pattern, start, end);
            if (!pattern.getPredicateVar().hasValue()
                    || !pattern.getPredicateVar().isAnonymous()) {
                this.renderingFailed("Unsupported path expression. Check node: ", node);
            }
            this.emitIf(inverse, "^").emitValue(pattern.getPredicateVar().getValue());

        } else if (node instanceof ArbitraryLengthPath) {

            // handles elt*, elt+
            final ArbitraryLengthPath path = (ArbitraryLengthPath) node;
            Preconditions.checkArgument(path.getMinLength() <= 1, "Invalid path length");
            this.emitPropertyPath(path.getPathExpression(), start, end)
                    .emit(path.getMinLength() == 0 ? "*" : "+");

        } else if (node instanceof Union) {

            // handles elt?, elt1|elt2|...
            final Union union = (Union) node;
            if (union.getLeftArg() instanceof ZeroLengthPath) {
                this.emitPropertyPath(union.getRightArg(), start, end).emit("?");
            } else if (union.getRightArg() instanceof ZeroLengthPath) {
                this.emitPropertyPath(union.getLeftArg(), start, end).emit("?");
            } else {
                this.emitPropertyPath(union.getLeftArg(), start, end);
                this.emit("|");
                this.emitPropertyPath(union.getRightArg(), start, end);
            }

        } else if (node instanceof Filter) {

            // handles !iri, !(iri1,iri2,...) with possibly inverse properties
            final Filter filter = (Filter) node;

            Preconditions.checkArgument(filter.getArg() instanceof StatementPattern);
            final StatementPattern pattern = (StatementPattern) filter.getArg();
            final boolean inverse = this.isInversePath(pattern, start, end);
            Preconditions.checkArgument(!pattern.getPredicateVar().hasValue()
                    && pattern.getPredicateVar().isAnonymous());

            final Set<IRI> negatedProperties = Sets.newLinkedHashSet();
            this.extractNegatedProperties(filter.getCondition(), negatedProperties);

            if (negatedProperties.size() == 1) {
                this.emit("!").emitIf(inverse, "^")
                        .emitValue(Iterables.getOnlyElement(negatedProperties));
            } else {
                this.emit("!(");
                boolean first = true;
                for (final IRI negatedProperty : negatedProperties) {
                    this.emitIf(!first, "|").emitIf(inverse, "^").emitValue(negatedProperty);
                    first = false;
                }
                this.emit(")");
            }

        }

        return this.emitIf(parenthesis, ")");
    }

    private void extractNegatedProperties(final ValueExpr condition,
            final Set<IRI> negatedProperties)
    {
        if (condition instanceof And) {
            final And and = (And) condition;
            this.extractNegatedProperties(and.getLeftArg(), negatedProperties);
            this.extractNegatedProperties(and.getRightArg(), negatedProperties);

        } else if (condition instanceof Compare) {
            final Compare compare = (Compare) condition;
            Preconditions.checkArgument(compare.getOperator() == CompareOp.NE);
            if (compare.getLeftArg() instanceof ValueConstant) {
                Preconditions.checkArgument(compare.getRightArg() instanceof Var);
                negatedProperties.add((IRI) ((ValueConstant) compare.getLeftArg()).getValue());
            } else if (compare.getRightArg() instanceof ValueConstant) {
                Preconditions.checkArgument(compare.getLeftArg() instanceof Var);
                negatedProperties.add((IRI) ((ValueConstant) compare.getRightArg()).getValue());
            } else {
                this.renderingFailed("Unsupported path expression. Check condition node: ",
                        condition);
            }
        }
    }

    private boolean isInversePath(final StatementPattern node, final Var start, final Var end)
    {
        if (node.getSubjectVar() == start) {
            Preconditions.checkArgument(node.getObjectVar() == end);
            return false;
        } else if (node.getObjectVar() == start) {
            Preconditions.checkArgument(node.getSubjectVar() == start);
            return true;
        } else {
            this.renderingFailed("Unsupported path expression. Check node: ", node);
            return false;
        }
    }

    // RENDERING OF VALUE EXPRESSIONS

    private SparqlRenderer emitValueExpr(final ValueExpr node)
    {
        final int nodePriority = MoreObjects.firstNonNull(
                SparqlRenderer.VALUE_OPERATOR_PRIORITIES.get(node.getClass()), Integer.MAX_VALUE);
        final int parentPriority = node.getParentNode() == null ? Integer.MIN_VALUE
                : MoreObjects.firstNonNull(SparqlRenderer.VALUE_OPERATOR_PRIORITIES
                        .get(node.getParentNode().getClass()), Integer.MIN_VALUE);

        this.emitIf(parentPriority > nodePriority, "(");

        if (node instanceof ValueConstant) {
            this.emitValue(((ValueConstant) node).getValue());

        } else if (node instanceof Var) {
            final Var var = (Var) node;
            if (var.getValue() != null) {
                this.emitValue(var.getValue());
            } else if (var.isAnonymous()) {
                this.emit(1 == this.variableRefCount.get(var.getName()) ? "[]"
                        : "?anon_var_" + var.getName().substring(1).replaceAll("-", "_"));
            } else {
                this.emit("?" + var.getName());
            }

        } else if (node instanceof UnaryValueOperator) {
            this.emitUnaryValueOperator((UnaryValueOperator) node);
        } else if (node instanceof BinaryValueOperator) {
            this.emitBinaryValueOperator((BinaryValueOperator) node);

        } else if (node instanceof FunctionCall) {
            final FunctionCall call = (FunctionCall) node;
            if (call.getURI().startsWith(FN.NAMESPACE)) {
                this.emitFunction(call.getURI().substring(FN.NAMESPACE.length()), call.getArgs());
            } else {
                this.emit(call.getURI()).emitFunction("", call.getArgs());
                // emitValue(new URIImpl(call.getURI())).emitFunction("", call.getArgs());
            }
        } else if (node instanceof Bound) {
            this.emitFunction("bound", Collections.<ValueExpr>singleton(((Bound) node).getArg()));
        } else if (node instanceof BNodeGenerator) {
            this.emitFunction("bnode",
                    Collections.<ValueExpr>singleton(((BNodeGenerator) node).getNodeIdExpr()));
        } else if (node instanceof Coalesce) {
            this.emitFunction("COALESCE", ((Coalesce) node).getArguments());
        } else if (node instanceof If) {
            this.emitFunction("IF", ImmutableList.<ValueExpr>of(((If) node).getCondition(), //
                    ((If) node).getResult(), ((If) node).getAlternative()));

        } else if (node instanceof Exists) {
            this.emit("EXISTS ").openBrace().emitTupleExpr(((Exists) node).getSubQuery())
                    .closeBrace();
        } else {
            this.renderingFailed("Unsupportedddd node: ", node);
        }

        return this.emitIf(parentPriority > nodePriority, ")");
    }

    private SparqlRenderer emitUnaryValueOperator(final UnaryValueOperator node)
    {
        if (node instanceof Not) {
            return this.emit(node.getArg() instanceof Exists ? "NOT " : "!")
                    .emitValueExpr(node.getArg());

        } else if (SparqlRenderer.VALUE_OPERATOR_NAMES.containsKey(node.getClass())) {
            this.emit(SparqlRenderer.VALUE_OPERATOR_NAMES.get(node.getClass())).emit("(");
            this.emitIf(
                    node instanceof AggregateOperator && ((AggregateOperator) node).isDistinct(),
                    "DISTINCT ");
            if (node instanceof Count && ((Count) node).getArg() == null) {
                this.emit("*");
            } else {
                this.emitValueExpr(node.getArg());
            }
            if (node instanceof GroupConcat && ((GroupConcat) node).getSeparator() != null) {
                this.emit(" ; separator=").emitValueExpr(((GroupConcat) node).getSeparator());
            }
            return this.emit(")");

        } else {
            return this.renderingFailed("Unsupported1 node: ", node);
        }
    }

    private SparqlRenderer emitBinaryValueOperator(final BinaryValueOperator node)
    {
        if (node instanceof And || node instanceof Or || node instanceof Compare
                || node instanceof MathExpr) {

            this.emitValueExpr(node.getLeftArg()).emit(" ");
            if (node instanceof Compare) {
                this.emit(
                        SparqlRenderer.COMPARE_OPERATOR_NAMES.get(((Compare) node).getOperator()));
            } else if (node instanceof MathExpr) {
                this.emit(((MathExpr) node).getOperator().getSymbol());
            } else {
                this.emit(SparqlRenderer.VALUE_OPERATOR_NAMES.get(node.getClass()));
            }
            return this.emit(" ").emitValueExpr(node.getRightArg());
            // GC

        } else if (node instanceof SameTerm) {
            return this.emit("sameTerm ").openBrace().emitNode(((SameTerm) node).getLeftArg())
                    .emit(", ").emitNode(((SameTerm) node).getRightArg()).emit(")");

        } else if (node instanceof LangMatches || node instanceof Regex) {

            this.emit(SparqlRenderer.VALUE_OPERATOR_NAMES.get(node.getClass())).emit("(");
            this.emitValueExpr(node.getLeftArg()).emit(", ").emitValueExpr(node.getRightArg());
            if (node instanceof Regex && ((Regex) node).getFlagsArg() != null) {
                this.emit(", ").emitValueExpr(((Regex) node).getFlagsArg());
            }
            return this.emit(")");

        } else {
            return this.renderingFailed("Unsupported2 node: ", node);
        }
    }

    private SparqlRenderer emitFunction(final String functionName,
            final Iterable<ValueExpr> arguments)
    {
        this.emit(functionName);
        this.emit("(");
        boolean first = true;
        for (final ValueExpr argument : arguments) {
            if (argument != null) {
                this.emitIf(!first, ", ");
                this.emitValueExpr(argument);
                first = false;
            }
        }
        return this.emit(")");
    }

}
