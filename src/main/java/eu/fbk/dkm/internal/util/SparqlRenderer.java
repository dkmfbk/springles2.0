package eu.fbk.dkm.internal.util;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.FN;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.Add;
import org.openrdf.query.algebra.AggregateOperator;
import org.openrdf.query.algebra.And;
import org.openrdf.query.algebra.ArbitraryLengthPath;
import org.openrdf.query.algebra.Avg;
import org.openrdf.query.algebra.BNodeGenerator;
import org.openrdf.query.algebra.BinaryTupleOperator;
import org.openrdf.query.algebra.BinaryValueOperator;
import org.openrdf.query.algebra.BindingSetAssignment;
import org.openrdf.query.algebra.Bound;
import org.openrdf.query.algebra.Clear;
import org.openrdf.query.algebra.Coalesce;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.Copy;
import org.openrdf.query.algebra.Count;
import org.openrdf.query.algebra.Create;
import org.openrdf.query.algebra.Datatype;
import org.openrdf.query.algebra.DeleteData;
import org.openrdf.query.algebra.Difference;
import org.openrdf.query.algebra.Distinct;
import org.openrdf.query.algebra.EmptySet;
import org.openrdf.query.algebra.Exists;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.Group;
import org.openrdf.query.algebra.GroupConcat;
import org.openrdf.query.algebra.IRIFunction;
import org.openrdf.query.algebra.If;
import org.openrdf.query.algebra.InsertData;
import org.openrdf.query.algebra.IsBNode;
import org.openrdf.query.algebra.IsLiteral;
import org.openrdf.query.algebra.IsNumeric;
import org.openrdf.query.algebra.IsURI;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Lang;
import org.openrdf.query.algebra.LangMatches;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.Load;
import org.openrdf.query.algebra.MathExpr;
import org.openrdf.query.algebra.Max;
import org.openrdf.query.algebra.Min;
import org.openrdf.query.algebra.Modify;
import org.openrdf.query.algebra.Move;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.Not;
import org.openrdf.query.algebra.Or;
import org.openrdf.query.algebra.Order;
import org.openrdf.query.algebra.OrderElem;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.Reduced;
import org.openrdf.query.algebra.Regex;
import org.openrdf.query.algebra.SameTerm;
import org.openrdf.query.algebra.Sample;
import org.openrdf.query.algebra.Service;
import org.openrdf.query.algebra.SingletonSet;
import org.openrdf.query.algebra.Slice;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.algebra.Str;
import org.openrdf.query.algebra.Sum;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.UnaryValueOperator;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.UpdateExpr;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.ZeroLengthPath;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

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
	 * @uml.property  name="nodes"
	 */
    private final Iterable<? extends QueryModelNode> nodes;

    /**
	 * @uml.property  name="datasets"
	 * @uml.associationEnd  multiplicity="(0 -1)" elementType="org.openrdf.query.Dataset"
	 */
    private final Iterable<? extends Dataset> datasets;

    /**
	 * @uml.property  name="referencedNamespaces"
	 * @uml.associationEnd  multiplicity="(0 -1)" elementType="java.lang.String"
	 */
    private Set<String> referencedNamespaces;

    /**
	 * @uml.property  name="baseURI"
	 */
    private String baseURI;

    /**
	 * @uml.property  name="prefixes"
	 * @uml.associationEnd  qualifier="getNamespace:java.lang.String java.lang.String"
	 */
    private Map<String, String> prefixes;

    /**
	 * @uml.property  name="indentLevel"
	 */
    private int indentLevel;

    /**
	 * @uml.property  name="indentString"
	 */
    private String indentString;

    /**
	 * @uml.property  name="linePrefix"
	 */
    private String linePrefix;

    /**
	 * @uml.property  name="headerRendered"
	 */
    private boolean headerRendered;

    /**
	 * @uml.property  name="currentDataset"
	 * @uml.associationEnd  
	 */
    private Dataset currentDataset;

    /**
	 * @uml.property  name="variableRefCount"
	 * @uml.associationEnd  qualifier="getName:java.lang.String java.lang.Integer"
	 */
    private Map<String, Integer> variableRefCount;

    /**
	 * @uml.property  name="lastPattern"
	 * @uml.associationEnd  
	 */
    private StatementPattern lastPattern;

    /**
	 * @uml.property  name="builder"
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
        this.indentString = DEFAULT_INDENT_STRING;
        this.linePrefix = DEFAULT_LINE_PREFIX;
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
            emit(this.linePrefix);
        }

        final Iterator<? extends Dataset> iterator = this.datasets.iterator();
        boolean first = true;
        for (final QueryModelNode node : this.nodes) {
            if (!first) {
                emit(";").newline().newline();
            }
            first = false;
            this.currentDataset = iterator.next();
            this.variableRefCount = collectVariableRefCount(node);
            emitNode(node);
        }

        if (this.headerRendered) {
            final String body = this.builder.toString();
            this.builder = builder;
            emit(this.linePrefix).emitHeader();
            this.builder.append(body);
        }
    }

    @Override
    public String toString()
    {
        final StringBuilder builder = new StringBuilder();
        toStringBuilder(builder);
        return builder.toString();
    }

    private static Map<String, Integer> collectVariableRefCount(final QueryModelNode rootNode)
    {
        final Map<String, Integer> map = Maps.newHashMap();
        rootNode.visit(new QueryModelVisitorBase<RuntimeException>() {

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
                emit("\\\\");
            } else if (c == '"') {
                emit("\\\"");
            } else if (c == '\n') {
                emit("\\n");
            } else if (c == '\r') {
                emit("\\r");
            } else if (c == '\t') {
                emit("\\t");
            } else if (c >= 0x0 && c <= 0x8 || c == 0xB || c == 0xC || c >= 0xE && c <= 0x1F
                    || c >= 0x7F && c <= 0xFFFF) {
                emit("\\u").emit(Strings.padStart(Integer.toHexString(c).toUpperCase(), 4, '0'));
            } else if (c >= 0x10000 && c <= 0x10FFFF) {
                emit("\\U").emit(Strings.padStart(Integer.toHexString(c).toUpperCase(), 8, '0'));
            } else {
                emit(Character.toString(c));
            }
        }
        return this;
    }

    private SparqlRenderer emitValue(final Value value)
    {
        if (value instanceof BNode) {
            return emit("_:").emit(((BNode) value).getID());

        } else if (value instanceof URI) {
            final URI uri = (URI) value;
            final String prefix = this.prefixes.get(uri.getNamespace());
            if (prefix != null) {
                if (this.referencedNamespaces != null) {
                    this.referencedNamespaces.add(uri.getNamespace());
                }
                return emit(prefix).emit(":").emit(uri.getLocalName());
            } else if (this.baseURI != null && uri.toString().startsWith(this.baseURI)) {
                return emit("<").emitEscaped(uri.toString().substring(this.baseURI.length()))
                        .emit(">");
            } else {
                return emit("<").emitEscaped(uri.toString()).emit(">");
            }

        } else if (value instanceof Literal) {
            final Literal literal = (Literal) value;
            if (XMLSchema.INTEGER.equals(literal.getDatatype())) {
                emit(Integer.parseInt(literal.getLabel().startsWith("+") ? literal.getLabel()
                        .substring(1) : literal.getLabel()) + "");
            } else {
                emit("\"").emit(literal.getLabel()).emit("\"");
                if (literal.getDatatype() != null) {
                    emit("^^").emitValue(literal.getDatatype());
                } else if (literal.getLanguage() != null) {
                    emit("@").emit(literal.getLanguage());
                }
            }
            return this;

        } else {
            throw new Error("Unexpected value: " + value);
        }
    }

    private SparqlRenderer newline()
    {
        emit("\n");
        emit(this.linePrefix);
        for (int i = 0; i < this.indentLevel; ++i) {
            emit(this.indentString);
        }
        return this;
    }

    private SparqlRenderer openBrace()
    {
        emit("{");
        ++this.indentLevel;
        newline();
        return this;
    }

    private SparqlRenderer closeBrace()
    {
        --this.indentLevel;
        newline();
        emit("}");
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
                emit("PREFIX ").emit(this.prefixes.get(namespace)).emit(": <")
                        .emitEscaped(namespace).emit(">").newline();
            }
            if (this.baseURI != null) {
                emit("BASE <").emitEscaped(this.baseURI).emit(">").newline();
            }
            newline();
        }
        return this;
    }

    // RENDERING OF NODES

    private SparqlRenderer emitNode(final QueryModelNode node)
    {
        if (node instanceof UpdateExpr) {
            emitUpdateExpr((UpdateExpr) node);
        } else if (node instanceof TupleExpr) {
            emitTupleExpr((TupleExpr) node);
        } else if (node instanceof ValueExpr) {
            emitValueExpr((ValueExpr) node);
        }
        return this;
    }

    // RENDERING OF UPDATE EXPRESSIONS

    private SparqlRenderer emitUpdateExpr(final UpdateExpr node)
    {
        Preconditions.checkArgument(!node.isSilent() || !(node instanceof InsertData)
                && !(node instanceof DeleteData) && !(node instanceof Modify),
                "SILENT unsupported for INSERT DATA / DELETE DATA / MODIFY");

        if (!(node instanceof Modify)) {
            emitDatasetAsComment(true, true).emit(UPDATE_OPERATOR_NAMES.get(node.getClass()))
                    .emit(" ").emitIf(node.isSilent(), "SILENT ");
        }

        if (node instanceof InsertData) {
            if (node instanceof Reduced
                    && ((Reduced)  node) //
                            .getArg() instanceof EmptySet) {
                openBrace().closeBrace();
            } else {
                openBrace().emitTupleExpr(
                        SparqlQuery.fromTupleExpr((TupleExpr) node, null)
                                .getConstructExpression()).closeBrace();
            }

        } else if (node instanceof DeleteData) {
            if ( node instanceof Reduced
                    && ((Reduced) node) //
                            .getArg() instanceof EmptySet) {
                openBrace().closeBrace();
            } else {
                openBrace().emitTupleExpr(
                        SparqlQuery.fromTupleExpr((TupleExpr)  node, null)
                                .getConstructExpression()).closeBrace();
            }

        } else if (node instanceof Modify) {
            final Modify modify = (Modify) node;
            if (modify.getDeleteExpr() != null) {
                emit("DELETE ").openBrace().emitTupleExpr(modify.getDeleteExpr()).closeBrace()
                        .newline();
            }
            if (modify.getInsertExpr() != null) {
                emit("INSERT ").openBrace().emitTupleExpr(modify.getInsertExpr()).closeBrace()
                        .newline();
            }
            if (this.currentDataset != null) {
                for (final URI uri : this.currentDataset.getDefaultGraphs()) {
                    emit("USING ").emitNode(new ValueConstant(uri)).newline();
                }
                for (final URI uri : this.currentDataset.getNamedGraphs()) {
                    emit("USING NAMED ").emitNode(new ValueConstant(uri)).newline();
                }
                emitDatasetAsComment(false, true);
            }
            emit("WHERE ").openBrace().emitTupleExpr(modify.getWhereExpr()).closeBrace();

        } else if (node instanceof Add) {
            final Add add = (Add) node;
            emitUpdateArg(add.getSourceGraph(), Scope.DEFAULT_CONTEXTS).newline();
            emit("TO ").emitUpdateArg(add.getDestinationGraph(), Scope.DEFAULT_CONTEXTS);

        } else if (node instanceof Clear) {
            emitUpdateArg(((Clear) node).getGraph(), ((Clear) node).getScope());

        } else if (node instanceof Copy) {
            final Copy copy = (Copy) node;
            emitUpdateArg(copy.getSourceGraph(), Scope.DEFAULT_CONTEXTS).newline();
            emit("TO ").emitUpdateArg(copy.getDestinationGraph(), Scope.DEFAULT_CONTEXTS);

        } else if (node instanceof Create) {
            emit("GRAPH ").emitNode(((Create) node).getGraph());

        } else if (node instanceof Load) {
            final Load load = (Load) node;
            emitValueExpr(load.getSource());
            if (load.getGraph() != null) {
                newline().emit("INTO GRAPH ").emitValueExpr(load.getGraph());
            }

        } else if (node instanceof Move) {
            final Move move = (Move) node;
            emitUpdateArg(move.getSourceGraph(), Scope.DEFAULT_CONTEXTS).newline();
            emit("TO ").emitUpdateArg(move.getDestinationGraph(), Scope.DEFAULT_CONTEXTS);
        }

        return this;
    }

    private SparqlRenderer emitDatasetAsComment(final boolean readPart, final boolean writePart)
    {
        if (this.currentDataset != null) {
            if (readPart) {
                for (final URI uri : this.currentDataset.getDefaultGraphs()) {
                    emit("# FROM ").emitValue(uri).newline();
                }
                for (final URI uri : this.currentDataset.getNamedGraphs()) {
                    emit("# FROM NAMED ").emitValue(uri).newline();
                }
            }
            if (writePart) {
                if (this.currentDataset.getDefaultInsertGraph() != null) {
                    emit("# INSERT INTO ").emitValue(this.currentDataset.getDefaultInsertGraph())
                            .newline();
                }
                for (final URI uri : this.currentDataset.getDefaultRemoveGraphs()) {
                    emit("# REMOVE FROM ").emitValue(uri).newline();
                }
            }
        }
        return this;
    }

    private SparqlRenderer emitUpdateArg(final ValueExpr graphExpr,
            final StatementPattern.Scope scopeIfGraphUnspecified)
    {
        if (graphExpr != null) {
            emit("GRAPH ").emitValueExpr(graphExpr);
        } else if (scopeIfGraphUnspecified == StatementPattern.Scope.DEFAULT_CONTEXTS) {
            emit("DEFAULT");
        } else if (scopeIfGraphUnspecified == StatementPattern.Scope.NAMED_CONTEXTS) {
            emit("NAMED");
        } else {
            emit("ALL");
        }
        return this;
    }

    // RENDERING OF TUPLE EXPRESSIONS

    private SparqlRenderer emitTupleExpr(final TupleExpr node)
    {
        if (node instanceof QueryRoot) {
            emitTupleExpr(((QueryRoot) node).getArg());

        } else if (node instanceof Projection || node instanceof MultiProjection
                || node instanceof Distinct || node instanceof Reduced || node instanceof Slice
                || node instanceof Order || node instanceof Group) {
            final SparqlQuery query = SparqlQuery.fromTupleExpr(node, this.currentDataset);
            if (node.getParentNode() != null) {
                openBrace().emitQuery(query).closeBrace();
            } else {
                emitQuery(query);
            }

        } else if (node instanceof UnaryTupleOperator) {
            emitUnaryTupleOperator((UnaryTupleOperator) node);

        } else if (node instanceof BinaryTupleOperator) {
            emitBinaryTupleOperator((BinaryTupleOperator) node);

        } else if (node instanceof StatementPattern) {
            emitStatementPattern((StatementPattern) node);

        } else if (node instanceof SingletonSet) {
            // nothing

        } else if (node instanceof EmptySet) {
            Preconditions.checkArgument(node.getParentNode() == null
                    || node.getParentNode() instanceof QueryRoot,
                    "Cannot translate EmptySet inside the body of a query / update operation");
            emit("CONSTRUCT {} WHERE {}");

        } else if (node instanceof ArbitraryLengthPath) {
            final ArbitraryLengthPath path = (ArbitraryLengthPath) node;
            emitPropertyPath(path, path.getSubjectVar(), path.getObjectVar());

        } else if (node instanceof BindingSetAssignment) {
            emitBindingSetAssignment((BindingSetAssignment) node);

        } else {
            renderingFailed("Unsupported3 node: ", node);
        }
        return this;
    }

    // RENDERING OF TUPLE EXPRESSIONS - QUERIES

    private SparqlRenderer emitQuery(final SparqlQuery query)
    {
        if (query.getQueryForm() == QueryForm.ASK) {
            emit("ASK");
        } else if (query.getQueryForm() == QueryForm.SELECT) {
            emit("SELECT ");
            if (query.getSelectModifier() != null) {
                emit(query.getSelectModifier().toString()).emit(" ");
            }
            emitProjectionElems(query.getSelectList());
        } else if (query.getQueryForm() == QueryForm.CONSTRUCT) {
            emit("CONSTRUCT ").openBrace().emitTupleExpr(query.getConstructExpression())
                    .closeBrace();
        }

        if (query.getFromDataset() != null) {
            for (final URI uri : query.getFromDataset().getDefaultGraphs()) {
                newline().emit("FROM ").emitValue(uri);
            }
            for (final URI uri : query.getFromDataset().getNamedGraphs()) {
                newline().emit("FROM NAMED ").emitValue(uri);
            }
        }

        newline().emit("WHERE ").openBrace().emitTupleExpr(query.getWhereExpression())
                .closeBrace();

        if (!query.getGroupByList().isEmpty()) {
            newline().emit("GROUP BY ").emitProjectionElems(query.getGroupByList());
        }

        if (query.getHavingCondition() != null) {
            newline().emit("HAVING (").emitValueExpr(query.getHavingCondition()).emit(")");
        }

        if (!query.getOrderByList().isEmpty()) {
            newline().emit("ORDER BY ").emitOrderElems(query.getOrderByList());
        }

        if (query.getQueryForm() != QueryForm.ASK) {
            if (query.getSliceOffset() != null) {
                newline().emit("OFFSET " + query.getSliceOffset());
            }
            if (query.getSliceLimit() != null) {
                newline().emit("LIMIT " + query.getSliceLimit());
            }
        }

        return this;
    }

    private SparqlRenderer emitProjectionElems(final Iterable<? extends ProjectionElem> elems)
    {
        boolean first = true;
        for (final ProjectionElem elem : elems) {
            emitIf(!first, " ");
            if (elem.getSourceExpression() != null) {
                emit("(").emitValueExpr(elem.getSourceExpression().getExpr()).emit(" AS ?")
                        .emit(elem.getTargetName()).emit(")");
            } else {
                emit("?").emit(elem.getTargetName());
            }
            first = false;
        }
        return this;
    }

    private SparqlRenderer emitOrderElems(final Iterable<? extends OrderElem> elems)
    {
        boolean first = true;
        for (final OrderElem orderElem : elems) {
            emitIf(!first, " ").emit(orderElem.isAscending() ? "asc(" : "desc(")
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
            emit("GRAPH ").emitValueExpr(node.getContextVar()).emit(" ").openBrace();
        }

        if (this.lastPattern == null) {
            // s p o
            emitValueExpr(node.getSubjectVar()).emit(" ").emitValueExpr(node.getPredicateVar())
                    .emit(" ").emitValueExpr(node.getObjectVar());
        } else if (!this.lastPattern.getSubjectVar().equals(node.getSubjectVar())) {
            // s1 p1 o1.\n s2 p2 o2
            emit(".").newline().emitValueExpr(node.getSubjectVar()).emit(" ")
                    .emitValueExpr(node.getPredicateVar()).emit(" ")
                    .emitValueExpr(node.getObjectVar());
        } else if (!this.lastPattern.getPredicateVar().equals(node.getPredicateVar())) {
            // s p1 o1;\n p2 o2
            emit(";").newline().emitValueExpr(node.getPredicateVar()).emit(" ")
                    .emitValueExpr(node.getObjectVar());
        } else if (!this.lastPattern.getObjectVar().equals(node.getObjectVar())) {
            // s p o1, o2
            emit(", ").emitValueExpr(node.getObjectVar());
        }

        this.lastPattern = null;
        if (!isLastPatternOfGraph(node)) {
            this.lastPattern = node;
        } else {
            if (insideGraph) {
                closeBrace();
            }
            if (!isLastPattern(node)) {
                newline();
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
        return !(node instanceof StatementPattern)
                || !Objects.equal(((StatementPattern) node).getContextVar(),
                        pattern.getContextVar());
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
            emitTupleExpr(filter.getArg()).newline().emit("FILTER");
            if (filter.getCondition() instanceof Exists || filter.getCondition() instanceof Not
                    && ((Not) filter.getCondition()).getArg() instanceof Exists) {
                emit(" ").emitNode(filter.getCondition());
            } else {
                emit(" (").emitValueExpr(filter.getCondition()).emit(")");
            }

        } else if (node instanceof Extension) {
            emitTupleExpr(node.getArg());
            for (final ExtensionElem elem : ((Extension) node).getElements()) {
                if (!(elem.getExpr() instanceof Var)
                        || !((Var) elem.getExpr()).getName().equals(elem.getName())) {
                    newline().emit("BIND (").emitValueExpr(elem.getExpr()).emit(" AS ?")
                            .emit(elem.getName()).emit(")");
                }
            }

        } else if (node instanceof Service) {
            final Service service = (Service) node;
            newline().emit("SERVICE ").emitIf(service.isSilent(), "SILENT ").openBrace()
                    .emitTupleExpr(service.getServiceExpr()).closeBrace().emit(" ")
                    .emitValueExpr(service.getServiceRef());

        } else {
            renderingFailed("Unsupported4 node: ", node);
        }
        return this;
    }

    private SparqlRenderer emitBinaryTupleOperator(final BinaryTupleOperator node)
    {
        if (node instanceof Join) {
            emitTupleExpr(node.getLeftArg()).emitTupleExpr(node.getRightArg());

        } else if (node instanceof LeftJoin) {
            final LeftJoin join = (LeftJoin) node;
            emitTupleExpr(join.getLeftArg()).newline().emit("OPTIONAL ").openBrace()
                    .emitTupleExpr(join.getRightArg());
            if (join.getCondition() != null) {
                newline().emit("FILTER");
                if (join.getCondition() instanceof Exists || join.getCondition() instanceof Not
                        && ((Not) join.getCondition()).getArg() instanceof Exists) {
                    emit(" ").emitNode(join.getCondition());
                } else {
                    emit(" (").emitValueExpr(join.getCondition()).emit(")");
                }
            }
            closeBrace();

        } else if (node instanceof Difference) {
            openBrace().emitTupleExpr(node.getLeftArg()).closeBrace().emit(" MINUS ").openBrace()
                    .emitTupleExpr(node.getRightArg()).closeBrace();

        } else if (node instanceof Union) {
            ZeroLengthPath path = null;
            if (node.getLeftArg() instanceof ZeroLengthPath) {
                path = (ZeroLengthPath) node.getLeftArg();
            } else if (node.getRightArg() instanceof ZeroLengthPath) {
                path = (ZeroLengthPath) node.getRightArg();
            }
            if (path == null) {
                openBrace().emitTupleExpr(node.getLeftArg()).closeBrace().emit(" UNION ")
                        .openBrace().emitTupleExpr(node.getRightArg()).closeBrace();
            } else {
                if (path.getContextVar() != null) {
                    emit("GRAPH ").emitValueExpr(path.getContextVar()).emit(" ").openBrace();
                }
                emitValueExpr(path.getSubjectVar()).emit(" ")
                        .emitPropertyPath(node, path.getSubjectVar(), path.getObjectVar())
                        .emit(" ").emitValueExpr(path.getObjectVar());
                if (path.getContextVar() != null) {
                    closeBrace();
                }
            }
        }
        return this;
    }

    private SparqlRenderer emitBindingSetAssignment(final BindingSetAssignment node)
    {
        if (node.getBindingNames().size() == 0) {
            return newline().emit("VALUES {}");

        } else if (node.getBindingNames().size() == 1) {
            final String name = Iterables.getOnlyElement(node.getBindingNames());
            newline().emit("VALUES ?").emit(name).emit(" ").openBrace();
            boolean first = true;
            for (final BindingSet bindingSet : node.getBindingSets()) {
                emitIf(!first, " ").emitValue(bindingSet.getValue(name));
                first = false;
            }
            return closeBrace();

        } else {
            newline().emit("VALUES (");
            boolean first = true;
            for (final String name : node.getBindingNames()) {
                emitIf(!first, " ").emit("?").emit(name);
                first = false;
            }
            emit(") ").openBrace();
            for (final BindingSet bindingSet : node.getBindingSets()) {
                emit("(");
                first = true;
                for (final String name : node.getBindingNames()) {
                    emitIf(!first, " ").emitValue(bindingSet.getValue(name));
                    first = false;
                }
                emit(")").newline();
            }
            return closeBrace();
        }
    }

    // RENDERING OF TUPLE EXPRESSIONS - PROPERTY PATHS

    private SparqlRenderer emitPropertyPath(final TupleExpr node, final Var start, final Var end)
    {
        // Note: elt1 / elt2 and ^(complex exp) do not occur in Sesame algebra

        final boolean parenthesis = node instanceof Union && node.getParentNode() != null
                && node.getParentNode() instanceof ArbitraryLengthPath;

        emitIf(parenthesis, "(");

        if (node instanceof StatementPattern) {

            // handles iri, ^iri
            final StatementPattern pattern = (StatementPattern) node;
            final boolean inverse = isInversePath(pattern, start, end);
            if (!pattern.getPredicateVar().hasValue() || !pattern.getPredicateVar().isAnonymous()) {
                renderingFailed("Unsupported path expression. Check node: ", node);
            }
            emitIf(inverse, "^").emitValue(pattern.getPredicateVar().getValue());

        } else if (node instanceof ArbitraryLengthPath) {

            // handles elt*, elt+
            final ArbitraryLengthPath path = (ArbitraryLengthPath) node;
            Preconditions.checkArgument(path.getMinLength() <= 1, "Invalid path length");
            emitPropertyPath(path.getPathExpression(), start, end).emit(
                    path.getMinLength() == 0 ? "*" : "+");

        } else if (node instanceof Union) {

            // handles elt?, elt1|elt2|...
            final Union union = (Union) node;
            if (union.getLeftArg() instanceof ZeroLengthPath) {
                emitPropertyPath(union.getRightArg(), start, end).emit("?");
            } else if (union.getRightArg() instanceof ZeroLengthPath) {
                emitPropertyPath(union.getLeftArg(), start, end).emit("?");
            } else {
                emitPropertyPath(union.getLeftArg(), start, end);
                emit("|");
                emitPropertyPath(union.getRightArg(), start, end);
            }

        } else if (node instanceof Filter) {

            // handles !iri, !(iri1,iri2,...) with possibly inverse properties
            final Filter filter = (Filter) node;

            Preconditions.checkArgument(filter.getArg() instanceof StatementPattern);
            final StatementPattern pattern = (StatementPattern) filter.getArg();
            final boolean inverse = isInversePath(pattern, start, end);
            Preconditions.checkArgument(!pattern.getPredicateVar().hasValue()
                    && pattern.getPredicateVar().isAnonymous());

            final Set<URI> negatedProperties = Sets.newLinkedHashSet();
            extractNegatedProperties(filter.getCondition(), negatedProperties);

            if (negatedProperties.size() == 1) {
                emit("!").emitIf(inverse, "^").emitValue(
                        Iterables.getOnlyElement(negatedProperties));
            } else {
                emit("!(");
                boolean first = true;
                for (final URI negatedProperty : negatedProperties) {
                    emitIf(!first, "|").emitIf(inverse, "^").emitValue(negatedProperty);
                    first = false;
                }
                emit(")");
            }

        }

        return emitIf(parenthesis, ")");
    }

    private void extractNegatedProperties(final ValueExpr condition,
            final Set<URI> negatedProperties)
    {
        if (condition instanceof And) {
            final And and = (And) condition;
            extractNegatedProperties(and.getLeftArg(), negatedProperties);
            extractNegatedProperties(and.getRightArg(), negatedProperties);

        } else if (condition instanceof Compare) {
            final Compare compare = (Compare) condition;
            Preconditions.checkArgument(compare.getOperator() == CompareOp.NE);
            if (compare.getLeftArg() instanceof ValueConstant) {
                Preconditions.checkArgument(compare.getRightArg() instanceof Var);
                negatedProperties.add((URI) ((ValueConstant) compare.getLeftArg()).getValue());
            } else if (compare.getRightArg() instanceof ValueConstant) {
                Preconditions.checkArgument(compare.getLeftArg() instanceof Var);
                negatedProperties.add((URI) ((ValueConstant) compare.getRightArg()).getValue());
            } else {
                renderingFailed("Unsupported path expression. Check condition node: ", condition);
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
            renderingFailed("Unsupported path expression. Check node: ", node);
            return false;
        }
    }

    // RENDERING OF VALUE EXPRESSIONS

    private SparqlRenderer emitValueExpr(final ValueExpr node)
    {
        final int nodePriority = Objects.firstNonNull(
                VALUE_OPERATOR_PRIORITIES.get(node.getClass()), Integer.MAX_VALUE);
        final int parentPriority = node.getParentNode() == null ? Integer.MIN_VALUE : Objects
                .firstNonNull(VALUE_OPERATOR_PRIORITIES.get(node.getParentNode().getClass()),
                        Integer.MIN_VALUE);

        emitIf(parentPriority > nodePriority, "(");

        if (node instanceof ValueConstant) {
            emitValue(((ValueConstant) node).getValue());

        } else if (node instanceof Var) {
            final Var var = (Var) node;
            if (var.getValue() != null) {
                emitValue(var.getValue());
            } else if (var.isAnonymous()) {
                emit(1 == this.variableRefCount.get(var.getName()) ? "[]" : "?anon_var_"
                        + var.getName().substring(1).replaceAll("-", "_"));
            } else {
                emit("?" + var.getName());
            }

        } else if (node instanceof UnaryValueOperator) {
            emitUnaryValueOperator((UnaryValueOperator) node);
        } else if (node instanceof BinaryValueOperator) {
            emitBinaryValueOperator((BinaryValueOperator) node);

        } else if (node instanceof FunctionCall) {
            final FunctionCall call = (FunctionCall) node;
            if (call.getURI().startsWith(FN.NAMESPACE)) {
                emitFunction(call.getURI().substring(FN.NAMESPACE.length()), call.getArgs());
            } else {
                emit(call.getURI()).emitFunction("", call.getArgs());
                // emitValue(new URIImpl(call.getURI())).emitFunction("", call.getArgs());
            }
        } else if (node instanceof Bound) {
            emitFunction("bound", Collections.<ValueExpr>singleton(((Bound) node).getArg()));
        } else if (node instanceof BNodeGenerator) {
            emitFunction("bnode",
                    Collections.<ValueExpr>singleton(((BNodeGenerator) node).getNodeIdExpr()));
        } else if (node instanceof Coalesce) {
            emitFunction("COALESCE", ((Coalesce) node).getArguments());
        } else if (node instanceof If) {
            emitFunction("IF", ImmutableList.<ValueExpr>of(((If) node).getCondition(), //
                    ((If) node).getResult(), ((If) node).getAlternative()));

        } else if (node instanceof Exists) {
            emit("EXISTS ").openBrace().emitTupleExpr(((Exists) node).getSubQuery()).closeBrace();
        } else {
            renderingFailed("Unsupportedddd node: ", node);
        }

        return emitIf(parentPriority > nodePriority, ")");
    }

    private SparqlRenderer emitUnaryValueOperator(final UnaryValueOperator node)
    {
        if (node instanceof Not) {
            return emit(node.getArg() instanceof Exists ? "NOT " : "!").emitValueExpr(
                    node.getArg());

        } else if (VALUE_OPERATOR_NAMES.containsKey(node.getClass())) {
            emit(VALUE_OPERATOR_NAMES.get(node.getClass())).emit("(");
            emitIf(node instanceof AggregateOperator && ((AggregateOperator) node).isDistinct(),
                    "DISTINCT ");
            if (node instanceof Count && ((Count) node).getArg() == null) {
                emit("*");
            } else {
                emitValueExpr(node.getArg());
            }
            if (node instanceof GroupConcat && ((GroupConcat) node).getSeparator() != null) {
                emit(" ; separator=").emitValueExpr(((GroupConcat) node).getSeparator());
            }
            return emit(")");

        } else {
            return renderingFailed("Unsupported1 node: ", node);
        }
    }

    private SparqlRenderer emitBinaryValueOperator(final BinaryValueOperator node)
    {
        if (node instanceof And || node instanceof Or || node instanceof Compare
                || node instanceof MathExpr) {

            emitValueExpr(node.getLeftArg()).emit(" ");
            if (node instanceof Compare) {
                emit(COMPARE_OPERATOR_NAMES.get(((Compare) node).getOperator()));
            } else if (node instanceof MathExpr) {
                emit(((MathExpr) node).getOperator().getSymbol());
            } else {
                emit(VALUE_OPERATOR_NAMES.get(node.getClass()));
            }
            return emit(" ").emitValueExpr(node.getRightArg());
 //GC         
            
        } else if (node instanceof SameTerm) {
         return   emit("sameTerm ").openBrace().emitNode( ((SameTerm) node).getLeftArg() ).emit(", ").emitNode(((SameTerm) node).getRightArg()).emit(")");
  
        } else if (node instanceof LangMatches || node instanceof Regex) {

            emit(VALUE_OPERATOR_NAMES.get(node.getClass())).emit("(");
            emitValueExpr(node.getLeftArg()).emit(", ").emitValueExpr(node.getRightArg());
            if (node instanceof Regex && ((Regex) node).getFlagsArg() != null) {
                emit(", ").emitValueExpr(((Regex) node).getFlagsArg());
            }
            return emit(")");

        } else {
            return renderingFailed("Unsupported2 node: ", node);
        }
    }

    private SparqlRenderer emitFunction(final String functionName,
            final Iterable<ValueExpr> arguments)
    {
        emit(functionName);
        emit("(");
        boolean first = true;
        for (final ValueExpr argument : arguments) {
            if (argument != null) {
                emitIf(!first, ", ");
                emitValueExpr(argument);
                first = false;
            }
        }
        return emit(")");
    }

}
