package eu.fbk.dkm.internal.util;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.FN;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Add;
import org.openrdf.query.algebra.And;
import org.openrdf.query.algebra.ArbitraryLengthPath;
import org.openrdf.query.algebra.Clear;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.Copy;
import org.openrdf.query.algebra.Create;
import org.openrdf.query.algebra.DeleteData;
import org.openrdf.query.algebra.EmptySet;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.InsertData;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Load;
import org.openrdf.query.algebra.Modify;
import org.openrdf.query.algebra.Move;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.Not;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.Reduced;
import org.openrdf.query.algebra.SingletonSet;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.algebra.Str;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.UpdateExpr;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.ZeroLengthPath;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.query.parser.sparql.SPARQLParser;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;

/**
 * Utility methods pertaining to Sesame query and update algebra.
 */
public final class Algebra
{

    /** Prevents class instantiation. */
    private Algebra()
    {
    }

    /**
     * Clones the supplied dataset, returning a modifiable copy. The returned dataset has the same
     * default graphs, named graphs, default insert graph and default remove graphs as the
     * supplied dataset.
     * 
     * @param dataset
     *            the dataset to clone
     * @return a modifiable clone of the supplied dataset, or <tt>null</tt> if the supplied
     *         dataset was null
     */
    public static DatasetImpl clone(@Nullable final Dataset dataset)
    {
        if (dataset == null) {
            return null;
        } else {
            final DatasetImpl ds = new DatasetImpl();
            for (final URI uri : dataset.getDefaultGraphs()) {
                ds.addDefaultGraph(uri);
            }
            for (final URI uri : dataset.getNamedGraphs()) {
                ds.addNamedGraph(uri);
            }
            for (final URI uri : dataset.getDefaultRemoveGraphs()) {
                ds.addDefaultRemoveGraph(uri);
            }
            ds.setDefaultInsertGraph(dataset.getDefaultInsertGraph());
            return ds;
        }
    }

    /**
     * Clones the supplied algebraic tree, optionally keeping track of the association between
     * original and cloned nodes. Note that associations must be stored in
     * <tt>IdentityHashMap</tt>s, as the implementation of <tt>equals</tt> and <tt>hashCode</tt>
     * for algebraic nodes is expensive as recursive and for some nodes broken (e.g.
     * <tt>Count</tt> node).
     * 
     * @param root
     *            the root of the tree to clone
     * @param sourceToCopyMap
     *            an optional map where associations original node =&gt; cloned node are stored
     * @param copyToSourceMap
     *            an optional map where associations cloned node =&gt; original node are stored
     * @param <T>
     *            the type of root node for the original and cloned trees
     * @return the root of the cloned tree
     */
    public static <T extends QueryModelNode> T clone(final T root,
            @Nullable final IdentityHashMap<QueryModelNode, QueryModelNode> sourceToCopyMap,
            @Nullable final IdentityHashMap<QueryModelNode, QueryModelNode> copyToSourceMap)
    {
        final List<QueryModelNode> sourceList = Lists.newArrayList();
        root.visit(new QueryModelVisitorBase<RuntimeException>() {

            @Override
            protected void meetNode(final QueryModelNode node) throws RuntimeException
            {
                sourceList.add(node);
                super.meetNode(node);
            }

        });

        @SuppressWarnings("unchecked")
        final T clone = (T) root.clone();
        clone.visit(new QueryModelVisitorBase<RuntimeException>() {

            private int index = 0;

            @Override
            protected void meetNode(final QueryModelNode node) throws RuntimeException
            {
                final QueryModelNode source = sourceList.get(this.index++);
                if (sourceToCopyMap != null) {
                    sourceToCopyMap.put(source, node);
                }
                if (copyToSourceMap != null) {
                    copyToSourceMap.put(node, source);
                }
                super.meetNode(node);
            }

        });

        return clone;
    }

    /**
     * Finds the root node of a property path, starting from a child node of that path.
     * 
     * @param childNode
     *            the child node where to start searching from
     * @return the root node of the property path, if there is such a path, or <tt>null</tt> if
     *         the child node does not belong to a property path
     */
    @Nullable
    public static TupleExpr findPropertyPathRoot(final QueryModelNode childNode)
    {
        TupleExpr root = null;
        for (QueryModelNode parent = childNode.getParentNode(); parent != null; parent = parent
                .getParentNode()) {
            if (parent instanceof ArbitraryLengthPath || parent instanceof Union
                    && (((Union) parent).getLeftArg() instanceof ZeroLengthPath //
                    || ((Union) parent).getRightArg() instanceof ZeroLengthPath)) {
                root = (TupleExpr) parent;
            }
        }
        return root;
    }

    /**
     * Finds the root node of a group pattern, starting from a child node of that pattern. A group
     * pattern is a sub-tree composed only of {@link Join} and {@link StatementPattern} nodes,
     * plus {@link ValueExpr} nodes linked to that nodes (e.g., variables).
     * 
     * @param childNode
     *            the child node where to start searching from
     * @return the root node of the group pattern comprising the specified child node, if any,
     *         otherwise <tt>null</tt>
     */
    @Nullable
    public static TupleExpr findGroupPatternRoot(final QueryModelNode childNode)
    {
        Preconditions.checkNotNull(childNode);

        QueryModelNode node = childNode;
        while (!(node instanceof TupleExpr) && node != null) {
            node = node.getParentNode();
        }
        if (node != null) {
            node = Objects.firstNonNull(findPropertyPathRoot(node), node);
            while (node != null && node.getParentNode() instanceof Join) {
                node = node.getParentNode();
            }
        }
        return (TupleExpr) node;
    }

    /**
     * Replaces a node in an algebraic tree, returning the root of the modified tree. Note the
     * root changes if the node to be replaced was the root of the tree.
     * 
     * @param root
     *            the root of the tree the node to replace belongs to
     * @param current
     *            the node to replace
     * @param replacement
     *            the replacement node
     * @return the root of the modified tree, possibly different from the supplied root
     */
    public static QueryModelNode replaceNode(final QueryModelNode root,
            final QueryModelNode current, final QueryModelNode replacement)
    {
        Preconditions.checkNotNull(root);
        Preconditions.checkNotNull(replacement);

        final QueryModelNode parent = current.getParentNode();
        if (parent == null) {
            return replacement;
        } else {
            parent.replaceChildNode(current, replacement);
            return root;
        }
    }

    /**
     * Inserts or modifies a <tt>Filter</tt> node before the node supplied, enforcing the
     * condition specified and returning the root of the modified tree. If a filter node is
     * already present, its condition is put in and with the supplied condition.
     * 
     * @param root
     *            the root of the tree the node to filter belongs to
     * @param filteredNode
     *            the node to be filtered
     * @param filterCondition
     *            the condition to enforce in the added (or existing) <tt>Filter</tt> node
     * @return the root of the modified tree, different from the original root if that was equal
     *         to the node to filter
     */
    public static QueryModelNode insertFilter(final QueryModelNode root,
            final TupleExpr filteredNode, final ValueExpr filterCondition)
    {
        Preconditions.checkNotNull(root);
        Preconditions.checkNotNull(filterCondition);

        final QueryModelNode parent = filteredNode.getParentNode();

        if (parent == null) {
            return new Filter(filteredNode, filterCondition);
        }

        if (parent instanceof Filter) {
            final ValueExpr existingCondition = ((Filter) parent).getCondition();
            parent.replaceChildNode(existingCondition, new And(existingCondition.clone(),
                    filterCondition));
        } else {
            parent.replaceChildNode(filteredNode, new Filter(filteredNode, filterCondition));
        }
        return root;
    }

    /**
     * Parses a SPARQL <tt>TupleExpr</tt>.
     * 
     * @param string
     *            the SPARQL string containing the expression
     * @param baseURI
     *            an optional base URI used to resolve relative URIs in the string
     * @param namespaces
     *            the namespaces to be used to resolve QNames in the string
     * @return the parsed <tt>TupleExpr</tt>
     * @throws MalformedQueryException
     *             in case the supplied string is malformed
     */
    public static TupleExpr parseTupleExpr(final String string, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces) throws MalformedQueryException
    {
        Preconditions.checkNotNull(string);

        final StringBuilder builder = new StringBuilder();

        if (namespaces != null) {
            for (final Map.Entry<String, String> entry : namespaces.entrySet()) {
                builder.append("PREFIX ").append(entry.getKey()).append(": ")
                        .append(SparqlRenderer.render(new URIImpl(entry.getValue()))).append("\n");
            }
        }

        builder.append("SELECT *\nWHERE {\n").append(string).append("\n}");

        try {
            return ((Projection) new SPARQLParser().parseQuery(builder.toString(), baseURI)
                    .getTupleExpr()).getArg();
        } catch (final MalformedQueryException ex) {
            throw new MalformedQueryException("Invalid tuple expr:\n" + string, ex);
        }
    }

    /**
     * Parses a SPARQL <tt>ValueExpr</tt>.
     * 
     * @param string
     *            the SPARQL string corresponding to the expression
     * @param baseURI
     *            an optional base URI used to resolve relative URIs in the string
     * @param namespaces
     *            the namespaces to be used to resolve QNames in the string
     * @return the parsed <tt>ValueExpr</tt>
     * @throws MalformedQueryException
     *             in case the supplied string is malformed
     */
    public static ValueExpr parseValueExpr(final String string, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces) throws MalformedQueryException
    {
        Preconditions.checkNotNull(string);

        final StringBuilder builder = new StringBuilder();

        if (namespaces != null) {
            for (final Map.Entry<String, String> entry : namespaces.entrySet()) {
                builder.append("PREFIX ").append(entry.getKey()).append(": ")
                        .append(SparqlRenderer.render(new URIImpl(entry.getValue()))).append("\n");
            }
        }

        builder.append("SELECT ((").append(string).append(") AS ?dummy) WHERE {}");

        final TupleExpr expr = new SPARQLParser().parseQuery(builder.toString(), baseURI)
                .getTupleExpr();
        return ((Extension) ((Projection) expr).getArg()).getElements().get(0).getExpr();
    }

    /**
     * Evaluates a <tt>ValueExpr</tt> using the bindings supplied and the <tt>ValueFactory</tt>
     * supplied.
     * 
     * @param expr
     *            the <tt>ValueExpr</tt> to evaluate
     * @param bindings
     *            the bindings for variables referenced in the <tt>ValueExpr</tt>
     * @param valueFactory
     *            the value factory used to create new nodes
     * @return the evaluation result
     * @throws QueryEvaluationException
     *             on failure
     */
    public static Value evaluateValueExpr(final ValueExpr expr, final BindingSet bindings,
            final ValueFactory valueFactory) throws QueryEvaluationException
    {
        return new EvaluationStrategyImpl(new TripleSource() {

            @Override
            public ValueFactory getValueFactory()
            {
                return valueFactory;
            }

            @Override
            public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
                    final Resource subj, final URI pred, final Value obj,
                    final Resource... contexts) throws QueryEvaluationException
            {
                return new EmptyIteration<Statement, QueryEvaluationException>();
            }

        }, null).evaluate(expr, bindings);
    }

    /**
     * Return a set with the name of variables mentioned in the algebraic expression specified.
     * More precisely, the method searches for all nodes of type {@link Var} which are not
     * associated to a value.
     * 
     * @param expr
     *            the expression from which variables should be extracted
     * @return an immutable set with the extracted variables
     */
    public static Set<String> extractVariables(final QueryModelNode expr)
    {
        final ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        expr.visit(new QueryModelVisitorBase<RuntimeException>() {

            @Override
            public void meet(final Var var) throws RuntimeException
            {
                if (!var.hasValue()) {
                    builder.add(var.getName());
                }
            }

        });
        return builder.build();
    }

    public static <T extends QueryModelNode> T replaceVariables(final T node,
            final Map<String, Var> substitution)
    {
        @SuppressWarnings("unchecked")
        final T result = (T) node.clone();
        result.visit(new QueryModelVisitorBase<RuntimeException>() {

            @Override
            public void meet(final Var var) throws RuntimeException
            {
                if (!var.hasValue()) {
                    final Var replacement = substitution.get(var.getName());
                    if (replacement != null) {
                        var.setName(replacement.getName());
                        var.setValue(replacement.getValue());
                        var.setAnonymous(replacement.isAnonymous());
                    }
                }
            }

        });
        return result;
    }

    public static List<StatementPattern> extractPatterns(final TupleExpr expr)
    {
        final ImmutableList.Builder<StatementPattern> builder = ImmutableList.builder();
        expr.visit(new QueryModelVisitorBase<RuntimeException>() {

            @Override
            public void meet(final StatementPattern pattern) throws RuntimeException
            {
                builder.add(pattern);
            }

        });
        return builder.build();
    }

    public static List<ValueExpr> extractConditions(final TupleExpr expr)
    {
        final ImmutableList.Builder<ValueExpr> builder = ImmutableList.builder();
        expr.visit(new QueryModelVisitorBase<RuntimeException>() {

            @Override
            public void meet(final Filter filter) throws RuntimeException
            {
                extractAndOperands(filter.getCondition());
            }

            private void extractAndOperands(final ValueExpr expr)
            {
                if (expr instanceof And) {
                    final And and = (And) expr;
                    extractAndOperands(and.getLeftArg());
                    extractAndOperands(and.getRightArg());
                } else {
                    builder.add(expr);
                }
            }

        });
        return builder.build();
    }

    /**
     * Checks whether the supplied algebraic expression is trivial, i.e., it produces no query
     * results or update modifications. Evaluation of a expression detected as trivial can be
     * safely skipped. The method operates on a best effort basis and it may be possible that a
     * trivial query is not detected as such. The following criteria are applied:
     * <ul>
     * <li>a {@link TupleExpr} is trivial if it can't read from any graph;</li>
     * <li>an {@link InsertData} or {@link DeleteData} is trivial if it inserts or deletes
     * nothing;</li>
     * <li>a {@link Modify} is trivial if its where part has no matches or if it deletes or insert
     * nothing;
     * <li>
     * </ul>
     * Note that {@link Clear}, {@link Add}, {@link Copy} and {@link Move} expressions are always
     * not trivial
     * 
     * @param expression
     *            the expression to check
     * @param dataset
     *            the optional dataset associated to the expression
     * @return <tt>true</tt> if the expression is trivial
     */
    public static boolean isTrivial(final QueryModelNode expression,
            @Nullable final Dataset dataset)
    {
        Preconditions.checkNotNull(expression);

        if (expression instanceof TupleExpr) {
            return dataset != null && dataset.getDefaultGraphs().isEmpty()
                    && dataset.getNamedGraphs().isEmpty();

        } else if (expression instanceof InsertData) {
            // Note: Sesame specific test - {} parsed as Reduced(EmptySet)
          //  final TupleExpr insertExpr = ((InsertData) expression).getInsertExpr();
            return expression instanceof Reduced
                    && ((Reduced) expression).getArg() instanceof EmptySet;

        } else if (expression instanceof DeleteData) {
            // Note: Sesame specific test - {} parsed as Reduced(EmptySet)
          //  final TupleExpr deleteExpr = ((DeleteData) expression).getDeleteExpr();
            return expression instanceof EmptySet || expression instanceof Reduced
                    && ((Reduced) expression).getArg() instanceof EmptySet;

        } else if (expression instanceof Modify) {
            // Note: Sesame specific tests - {} parsed as SingletonSet.
            final Modify modify = (Modify) expression;
            final TupleExpr insertExpr = modify.getInsertExpr();
            final TupleExpr deleteExpr = modify.getDeleteExpr();
            return (insertExpr == null || insertExpr instanceof SingletonSet)
                    && (deleteExpr == null || deleteExpr instanceof SingletonSet);
            // return dataset != null && dataset.getDefaultGraphs().isEmpty()
            // && dataset.getNamedGraphs().isEmpty()
            // || (insertExpr == null || insertExpr instanceof SingletonSet)
            // && (deleteExpr == null || deleteExpr instanceof SingletonSet);
        }

        return false;
    }

    /**
     * Rewrites the supplied update expression in order to re-target read and write access to
     * Sesame null context to a replacement context.
     * 
     * @param expr
     *            the expression to rewrite
     * @param dataset
     *            the optional dataset associated to the expression
     * @param replacement
     *            the replacement graph
     * @param <T>
     *            the type of expression to rewrite
     * @return a key-value pair whose key is the rewritten expression, and value is the rewritten
     *         dataset; note that both the expression and the dataset are rewritten only if
     *         necessary, reusing the original objects otherwise
     */
    @SuppressWarnings("unchecked")
    public static <T extends QueryModelNode> Entry<T, Dataset> rewriteDefaultContext(final T expr,
            @Nullable final Dataset dataset, final URI replacement)
    {
        Preconditions.checkNotNull(replacement);

        T resultExpr = expr;
        Dataset resultDataset = dataset;

        if (expr instanceof TupleExpr) {
            resultDataset = rewriteDefaultContext(resultDataset, replacement, false);

        } else if (expr instanceof Copy) {
            Copy copy = (Copy) expr;
            if (copy.getSourceGraph() == null || copy.getDestinationGraph() == null) {
                copy = copy.clone();
                copy.setSourceGraph(replaceIfNull(copy.getSourceGraph(), replacement));
                copy.setDestinationGraph(replaceIfNull(copy.getDestinationGraph(), replacement));
                resultExpr = (T) copy;
            }

        } else if (expr instanceof Move) {
            Move move = (Move) expr;
            if (move.getSourceGraph() == null || move.getDestinationGraph() == null) {
                move = move.clone();
                move.setSourceGraph(replaceIfNull(move.getSourceGraph(), replacement));
                move.setDestinationGraph(replaceIfNull(move.getDestinationGraph(), replacement));
                resultExpr = (T) move;
            }

        } else if (expr instanceof Add) {
            Add add = (Add) expr;
            if (add.getSourceGraph() == null || add.getDestinationGraph() == null) {
                add = add.clone();
                add.setSourceGraph(replaceIfNull(add.getSourceGraph(), replacement));
                add.setDestinationGraph(replaceIfNull(add.getDestinationGraph(), replacement));
                resultExpr = (T) add;
            }

        } else if (expr instanceof Load) {
            Load load = (Load) expr;
            if (load.getGraph() == null) {
                load = load.clone();
                load.setGraph(new ValueConstant(replacement));
                resultExpr = (T) load;
            }

        } else if (expr instanceof Clear) {
            // TODO: check behaviour w.r.t. closure
            Clear clear = (Clear) expr;
            if (clear.getScope() == null || clear.getScope() == Scope.DEFAULT_CONTEXTS) {
                clear = clear.clone();
                if (clear.getScope() == Scope.DEFAULT_CONTEXTS) {
                    clear.setGraph(replaceIfNull(clear.getGraph(), replacement));
                }
                clear.setScope(Scope.NAMED_CONTEXTS);
                resultExpr = (T) clear;
            }

        } else if (expr instanceof InsertData || expr instanceof DeleteData
                || expr instanceof Modify) {
            // Dataset considered only for INSERT DATA, DELETE DATA, MODIFY
            resultDataset = rewriteDefaultContext(dataset, replacement, true);

        } else {
            Preconditions.checkNotNull(expr);
            throw new Error("Unknown update expression: " + expr);
        }

        return new SimpleImmutableEntry<T, Dataset>(resultExpr, resultDataset);
    }

    private static Dataset rewriteDefaultContext(@Nullable final Dataset dataset,
            final URI replacement, final boolean rewriteInsertDeleteClauses)
    {
        Dataset resultDataset = dataset;

        if (dataset != null) {
            if (dataset.getDefaultGraphs().contains(null)) {
                resultDataset = clone(dataset);
                ((DatasetImpl) resultDataset).removeDefaultGraph(null);
                ((DatasetImpl) resultDataset).addDefaultGraph(replacement);
            }
            if (dataset.getNamedGraphs().contains(null)) {
                resultDataset = resultDataset != dataset ? resultDataset : clone(dataset);
                ((DatasetImpl) resultDataset).removeNamedGraph(null);
                ((DatasetImpl) resultDataset).addNamedGraph(replacement);
            }
        }

        if (rewriteInsertDeleteClauses) {
            if (resultDataset == null) {
                resultDataset = new DatasetImpl();
                ((DatasetImpl) resultDataset).setDefaultInsertGraph(replacement);
                ((DatasetImpl) resultDataset).addDefaultRemoveGraph(replacement);

            } else {
                if (dataset.getDefaultInsertGraph() == null) {
                    resultDataset = resultDataset != dataset ? resultDataset : clone(dataset);
                    ((DatasetImpl) resultDataset).setDefaultInsertGraph(replacement);
                }
                if (dataset.getDefaultRemoveGraphs().isEmpty()
                        || dataset.getDefaultRemoveGraphs().contains(null)) {
                    resultDataset = resultDataset != dataset ? resultDataset : clone(dataset);
                    ((DatasetImpl) resultDataset).removeDefaultRemoveGraph(null);
                    ((DatasetImpl) resultDataset).addDefaultRemoveGraph(replacement);
                }
            }
        }

        return resultDataset;
    }

    /**
     * Helper method that generates a new <tt>ValueConstant</tt> for a replacement graph if the
     * supplied <tt>ValueConstant</tt> is null, meaning it denotes the default graph.
     * 
     * @param constant
     *            the current <tt>ValueConstant</tt>
     * @param replacement
     *            the URI of the replacement graph
     * @return either the supplied <tt>ValueConstant</tt>, if not null, i.e., denoting the default
     *         graph, or a new <tt>ValueConstant</tt> for the replacement graph supplied
     */
    private static ValueConstant replaceIfNull(final ValueConstant constant, final URI replacement)
    {
        return constant != null ? constant : new ValueConstant(replacement);
    }

    /**
     * Rewrites an algebraic expression and/or its associated dataset in order to prevent reading
     * from graphs whose URIs matches the supplied prefix.
     * 
     * @param prefix
     *            the prefix of graphs for which reading must be prevented
     * @param expr
     *            the expression to rewrite
     * @param dataset
     *            the optional dataset associated to the expression
     * @return <tt>null</tt> if the rewritten expression is trivial, otherwise a key-value pair
     *         whose key is the rewritten expression, and value is the rewritten dataset; note
     *         that both the expression and dataset are rewritten only if necessary, reusing the
     *         supplied objects otherwise
     */
    @Nullable
    public static Entry<QueryModelNode, Dataset> excludeReadingGraphs(final URIPrefix prefix,
            final QueryModelNode expr, @Nullable final Dataset dataset)
    {
        Preconditions.checkNotNull(prefix);
        Preconditions.checkNotNull(expr);

        // For updates, default and named graphs parts of a dataset are considered only for MODIFY
        Dataset resultDataset = dataset;
        if (dataset != null && (expr instanceof TupleExpr || expr instanceof Modify)) {
            resultDataset = clone(dataset);
            final Predicate<Value> pred = prefix.valueMatcher();
            for (final URI uri : Iterables.filter(dataset.getDefaultGraphs(), pred)) {
                ((DatasetImpl) resultDataset).removeDefaultGraph(uri);
            }
            for (final URI uri : Iterables.filter(dataset.getNamedGraphs(), pred)) {
                ((DatasetImpl) resultDataset).removeNamedGraph(uri);
            }
        }

        QueryModelNode resultExpression = expr;
        if (resultDataset == null) {
            if (expr instanceof Add && matches(prefix, ((Add) expr).getSourceGraph())) {
                resultExpression = null;
            } else if (expr instanceof Copy && matches(prefix, ((Copy) expr).getSourceGraph())) {
                resultExpression = new Clear(((Copy) expr).getDestinationGraph());
            } else if (expr instanceof Add && matches(prefix, ((Add) expr).getSourceGraph())) {
                resultExpression = new Clear(((Add) expr).getDestinationGraph());
            } else if (expr instanceof Modify) {
                final Modify modify = (Modify) expr;
                final TupleExpr whereExpr = modify.getWhereExpr();
                final TupleExpr newWhereExpr = new ExcludeReadingGraphsVisitor(prefix)
                        .process(whereExpr);
                resultExpression = newWhereExpr == whereExpr ? expr : new Modify(
                        modify.getDeleteExpr(), modify.getInsertExpr(), newWhereExpr);
            } else if (expr instanceof TupleExpr) {
                resultExpression = new ExcludeReadingGraphsVisitor(prefix)
                        .process((TupleExpr) expr);
            }
        }

        return resultExpression == null || isTrivial(resultExpression, resultDataset) ? null
                : new SimpleImmutableEntry<QueryModelNode, Dataset>(resultExpression,
                        resultDataset);
    }

    /**
     * Rewrites an update expression and/or its associated dataset in order to prevent modifying
     * the content of graphs whose URIs matches the supplied prefix. <b>NOTE: {@link Modify}
     * expression with more than one unbound context variable in <tt>INSERT</tt> or
     * <tt>DELETE</tt> clauses that are assigned to URIs matching the pattern, are rewritten in a
     * way that may prevent also writing of statements to graphs not matching the prefix.</b>
     * 
     * @param prefix
     *            the prefix of graphs for which writing must be prevented
     * @param expr
     *            the update expression to rewrite
     * @param dataset
     *            the optional dataset associated to the expression
     * @return <tt>null</tt> if the rewritten expression is trivial, otherwise a key-value pair
     *         whose key is the rewritten update expression, and value is the rewritten dataset;
     *         note that both the expression and dataset are rewritten only if necessary, reusing
     *         the supplied objects otherwise
     */
    @Nullable
    public static Entry<UpdateExpr, Dataset> excludeWritingGraphs(final URIPrefix prefix,
            final UpdateExpr expr, @Nullable final Dataset dataset)
    {
        Preconditions.checkNotNull(prefix);
        Preconditions.checkNotNull(expr);

        UpdateExpr resultExpr = expr;
        Dataset resultDataset = dataset;

        // true if statement patterns in default insert/remvoe graph can be kept
        boolean defaultInsert = true;
        boolean defaultRemove = true;

        // Dataset is considered only for INSERT DATA, DELETE DATA, MODIFY. If dataset specifies
        // only inferred graphs as the default insert and remove graphs, we have to remove them
        // from the dataset and then discard all the statements in insert/remove clauses that
        // address the default graph, as otherwise they will end up in Sesame null context.
        if (dataset != null && (expr instanceof InsertData //
                || expr instanceof DeleteData || expr instanceof Modify)) {

            if (prefix.matches(dataset.getDefaultInsertGraph())) {
                resultDataset = clone(dataset);
                ((DatasetImpl) resultDataset).setDefaultInsertGraph(null);
                defaultInsert = false;
            }
            for (final URI uri : Iterables.filter(dataset.getDefaultRemoveGraphs(),
                    prefix.valueMatcher())) {
                resultDataset = resultDataset != dataset ? resultDataset : clone(dataset);
                ((DatasetImpl) resultDataset).removeDefaultRemoveGraph(uri);
            }
            defaultRemove = dataset.getDefaultRemoveGraphs().size() == 0
                    || resultDataset.getDefaultRemoveGraphs().size() > 0;
        }

        if (expr instanceof Copy) {
            resultExpr = matches(prefix, ((Copy) expr).getDestinationGraph()) ? null : expr;
        } else if (expr instanceof Move) {
            resultExpr = matches(prefix, ((Move) expr).getDestinationGraph()) ? null : expr;
        } else if (expr instanceof Add) {
            resultExpr = matches(prefix, ((Add) expr).getDestinationGraph()) ? null : expr;
        } else if (expr instanceof Load) {
            resultExpr = matches(prefix, ((Load) expr).getGraph()) ? null : expr;
        } else if (expr instanceof Create) {
            resultExpr = matches(prefix, ((Create) expr).getGraph()) ? null : expr;
        } else if (expr instanceof Clear) {
            resultExpr = matches(prefix, ((Clear) expr).getGraph()) ? null : expr;

        } else if (expr instanceof InsertData) {
           // final TupleExpr data = expr;
            final TupleExpr newData = rewriteData(prefix, !defaultInsert, (TupleExpr)expr, null);
            if (newData != (TupleExpr)expr) {
                resultExpr = new InsertData( ((InsertData)newData).getDataBlock() );
            }

        } else if (expr instanceof DeleteData) {
         ///   final TupleExpr data = ((DeleteData) expr).getDeleteExpr();
            final TupleExpr newData = rewriteData(prefix, !defaultRemove, (TupleExpr)expr, null);
            if (newData != (TupleExpr)expr) {
                resultExpr = new DeleteData(((DeleteData)newData).getDataBlock());
            }

        } else if (expr instanceof Modify) {
            // XXX: expression with more than one unbound context variable in INSERT or DELETE
            // clauses that - at evaluation time - are assigned to URIs matching the pattern, are
            // rewritten in a way that may prevent also writing of statements to graphs not
            // matching the supplied prefix
            final Modify modify = (Modify) expr;
            final Set<String> vars = Sets.newHashSet();
            final TupleExpr insertExpr = modify.getInsertExpr() == null ? null : rewriteData(
                    prefix, !defaultInsert, modify.getInsertExpr(), vars);
            final TupleExpr deleteExpr = modify.getDeleteExpr() == null ? null : rewriteData(
                    prefix, !defaultRemove, modify.getDeleteExpr(), vars);
            TupleExpr whereExpr = modify.getWhereExpr();
            if (!vars.isEmpty()) {
                whereExpr = whereExpr.clone();
                whereExpr = (TupleExpr) insertFilter(whereExpr, whereExpr,
                        excludeCondition(prefix, vars));
            }
            if (insertExpr != modify.getInsertExpr() || deleteExpr != modify.getDeleteExpr()
                    || whereExpr != modify.getWhereExpr()) {
                resultExpr = new Modify(deleteExpr, insertExpr, whereExpr);
            }
        }

        return resultExpr == null || isTrivial(resultExpr, resultDataset) ? null
                : new SimpleImmutableEntry<UpdateExpr, Dataset>(resultExpr, resultDataset);
    }

    /**
     * Helper method that checks whether a URI prefix applies to the URI possibly associated to a
     * <tt>ValueConstant</tt>.
     * 
     * @param prefix
     *            the URI prefix
     * @param expr
     *            the <tt>ValueConstant</tt> expression
     * @return <tt>true</tt> if match occurs
     */
    private static boolean matches(final URIPrefix prefix, final ValueConstant expr)
    {
        return expr != null && expr.getValue() instanceof URI && prefix.matches(expr.getValue());
    }

    /**
     * Helper method that returns a boolean condition enforcing that the values of all the
     * supplied variables must not match the URI prefix specified (if values are URIs).
     * 
     * @param prefix
     *            the URI prefix
     * @param varNames
     *            the names of the constrained variables
     * @return the created algebraic expression
     */
    private static ValueExpr excludeCondition(final URIPrefix prefix,
            final Iterable<String> varNames)
    {
        ValueExpr condition = null;
        for (final String varName : varNames) {
            ValueExpr test;
            if (prefix.isFullURI()) {
                test = new Compare(new Var(varName), new ValueConstant(prefix.asFullURI()),
                        Compare.CompareOp.NE);
            } else {
                test = new Not(new FunctionCall(FN.STARTS_WITH.stringValue(), new Str(new Var(
                        varName)), new ValueConstant(new LiteralImpl(prefix.getPrefix()))));
            }
            condition = condition == null ? test : new And(test, condition);
        }
        return condition;
    }

    /**
     * Helper methods that rewrites the group pattern of an <tt>INSERT</tt> or <tt>DELETE</tt>
     * clause, removing statement patterns addressing contexts whose URI matches the supplied or,
     * optionally, in the default context. The group pattern must be composed exclusively of
     * {@link StatementPattern} and {@link Join} nodes. In case it contains variables (not bound
     * to a value), those variables are added to the set supplied with parameter <tt>vars</tt>, as
     * it will be required to constrain their values in a <tt>WHERE</tt> clause in order to fully
     * avoid writing to the specified graphs.
     * 
     * @param prefix
     *            the URI prefix
     * @param excludeDefault
     *            <tt>true</tt> if statement patterns addressing the default graph must be removed
     * @param expr
     *            the group pattern to rewrite
     * @param vars
     *            a <tt>Set</tt> of variables populated with the variables (with unknown value)
     *            found in the group pattern
     * @return the rewritten group pattern, <tt>null</tt> in case the original pattern is
     *         completely erased; note that the supplied group pattern is returned unchanged if no
     *         modification was necessary
     */
    @Nullable
    private static TupleExpr rewriteData(final URIPrefix prefix, final boolean excludeDefault,
            final TupleExpr expr, @Nullable final Set<String> vars)
    {
        Preconditions.checkNotNull(prefix);

        if (expr instanceof StatementPattern) {
            final StatementPattern pattern = (StatementPattern) expr;
            if (pattern.getContextVar() == null) {
                return excludeDefault ? null : pattern;
            } else if (!pattern.getContextVar().hasValue()) {
                if (vars != null) {
                    vars.add(pattern.getContextVar().getName());
                }
                return pattern;
            } else if (!prefix.matches(pattern.getContextVar().getValue())) {
                return pattern;
            } else {
                return null;
            }

        } else if (expr instanceof Join) {
            final Join join = (Join) expr;
            final TupleExpr left = rewriteData(prefix, excludeDefault, join.getLeftArg(), vars);
            final TupleExpr right = rewriteData(prefix, excludeDefault, join.getRightArg(), vars);
            if (left == null) {
                return right;
            } else if (right == null) {
                return left;
            } else if (left != join.getLeftArg() || right != join.getRightArg()) {
                return new Join(left, right);
            } else {
                return join;
            }

        } else if (expr instanceof Reduced) {
            final Reduced reduced = (Reduced) expr;
            final TupleExpr arg = rewriteData(prefix, excludeDefault, reduced.getArg(), vars);
            if (arg == null) {
                return null;
            } else if (arg != reduced.getArg()) {
                return new Reduced(arg);
            } else {
                return reduced;
            }

        } else if (expr instanceof Projection && //
                ((Projection) expr).getArg() instanceof Extension) {

            final Projection projection = (Projection) expr;
            final Map<String, ValueExpr> map = indexExtensions((Extension) projection.getArg());
            return rewriteProjection(prefix, excludeDefault, projection.getProjectionElemList(),
                    map, vars) ? projection : null;

        } else if (expr instanceof MultiProjection
                && ((UnaryTupleOperator) expr).getArg() instanceof Extension) {

            final MultiProjection proj = (MultiProjection) expr;
            final Extension ext = (Extension) proj.getArg();
            final Map<String, ValueExpr> map = indexExtensions(ext);

            List<ProjectionElemList> rewrittenTuples = null;
            final int size = proj.getProjections().size();
            for (int i = 0; i < size; ++i) {
                final ProjectionElemList tuple = proj.getProjections().get(i);
                if (!rewriteProjection(prefix, excludeDefault, tuple, map, vars)) {
                    if (rewrittenTuples == null) {
                        rewrittenTuples = Lists.newArrayListWithCapacity(size);
                        rewrittenTuples.addAll(proj.getProjections().subList(0, i));
                    }
                } else if (rewrittenTuples != null) {
                    rewrittenTuples.add(tuple);
                }
            }

            return rewrittenTuples == null ? expr : new MultiProjection(ext, rewrittenTuples);

        } else {
            Preconditions.checkNotNull(expr);
            throw new Error("Unexpected node:" + expr);
        }
    }

    private static boolean rewriteProjection(final URIPrefix prefix, final boolean excludeDefault,
            final ProjectionElemList expr, final Map<String, ValueExpr> extensions,
            final Set<String> vars)
    {
        if (expr.getElements().size() == 4) {
            final String name = expr.getElements().get(3).getSourceName();
            final ValueExpr context = extensions.get(name);
            if (context instanceof ValueConstant) {
                if (prefix.matches(((ValueConstant) context).getValue())) {
                    return false;
                } else if (context instanceof Var) {
                    if (prefix.matches(((Var) context).getValue())) {
                        return false;
                    }
                }
            } else if (context instanceof Var) {
                vars.add(((Var) context).getName());
            }
        } else if (excludeDefault) {
            return false;
        }
        return true;
    }

    private static Map<String, ValueExpr> indexExtensions(final Extension extension)
    {
        final Map<String, ValueExpr> map = Maps.newHashMap();
        for (final ExtensionElem elem : extension.getElements()) {
            map.put(elem.getName(), elem.getExpr());
        }
        return map;
    }

    /**
	 * Helper class supporting the rewriting of  {@link TupleExpr}  so to prevent reading from graphs whose URIs match a certain prefix. <p> NOTE: this class is not thread safe. </p>
	 */
    private static class ExcludeReadingGraphsVisitor extends
            QueryModelVisitorBase<RuntimeException>
    {

        /**
		 * Helper class supporting the rewriting of  {@link TupleExpr}  so to prevent reading from graphs whose URIs match a certain prefix. <p> NOTE: this class is not thread safe. </p>
		 */
		private static class ExcludeReadingGraphsVisitor1 extends
		        QueryModelVisitorBase<RuntimeException>
		{
		
		    /**
			 * The URI prefix of graphs not to be read.
			 * @uml.property  name="prefix"
			 * @uml.associationEnd  
			 */
		    private final URIPrefix prefix;
		
		    /** The expression to be rewritten, at the beginning, then the rewritten expression. */
		    private TupleExpr expression;
		
		    /** A map with scheduled assignments of context variables to patterns in the expression. */
		    private Map<TupleExpr, String> assignments;
		
		    /** A map with scheduled filtering of context variables to be added in the expression. */
		    private Multimap<TupleExpr, String> filtering;
		
		    /** A set of nodes to be deleted. */
		    private Set<TupleExpr> deletions;
		
		    /** A counter used to generate the names of new context variables. */
		    private int counter;
		
		    /**
		     * Creates a new instance for the URI prefix supplied.
		     * 
		     * @param prefix
		     *            the URI prefix of graphs not to be read
		     */
		    public ExcludeReadingGraphsVisitor1(final URIPrefix prefix)
		    {
		        Preconditions.checkNotNull(prefix);
		        this.prefix = prefix;
		    }
		
		    /**
		     * Rewrites the expression specified, using the URI prefix provided at construction time.
		     * 
		     * @param expression
		     *            the expression to rewrite
		     * @return the rewritten expression, possibly the supplied expression itself
		     */
		    public TupleExpr process(final TupleExpr expression)
		    {
		        this.expression = expression;
		        this.assignments = Maps.newHashMap();
		        this.filtering = HashMultimap.create();
		        this.deletions = Sets.newHashSet();
		        this.counter = 0;
		
		        this.expression.visit(this);
		
		        return this.expression == null || isTrivial(this.expression, null) ? null
		                : (TupleExpr) this.expression;
		    }
		
		    /**
		     * {@inheritDoc} Updates the data structures holding scheduled modifications to the
		     * expression while visiting the expression tree. After the whole tree is visited, the
		     * rewritten expression is generated if some modification was scheduled.
		     */
		    @Override
		    protected void meetNode(final QueryModelNode node) throws RuntimeException
		    {
		        if (node instanceof StatementPattern) {
		            scheduleModifications((TupleExpr) node, ((StatementPattern) node).getContextVar());
		        } else if (node instanceof ZeroLengthPath) {
		            scheduleModifications((TupleExpr) node, ((ZeroLengthPath) node).getContextVar());
		        }
		
		        super.meetNode(node);
		
		        if (node == this.expression
		                && (!this.deletions.isEmpty() || !this.filtering.isEmpty())) {
		            generateExpression();
		        }
		    }
		
		    /**
		     * Analyzes the supplied pattern node and the associated context variable, scheduling the
		     * appropriate modifications to the algebraic expression.
		     * 
		     * @param node
		     *            the pattern node, either a {@link StatementPattern} or a
		     *            {@link ZeroLengthPath}
		     * @param var
		     *            the context variable associated to the node
		     */
		    private void scheduleModifications(final TupleExpr node, final Var var)
		    {
		        if (var == null) {
		            final String newVarName = "__c" + this.counter++;
		            this.assignments.put(node, newVarName);
		            this.filtering.put(findGroupPatternRoot(node), newVarName);
		
		        } else if (var.getValue() == null) {
		            this.filtering.put(findGroupPatternRoot(node), var.getName());
		
		        } else if (var.getValue() instanceof URI && this.prefix.matches(var.getValue())) {
		            this.deletions.add(node);
		        }
		    }
		
		    /**
		     * Generates the rewritten algebraic expression, if some modification was scheduled.
		     */
		    private void generateExpression()
		    {
		        final IdentityHashMap<QueryModelNode, QueryModelNode> map = Maps.newIdentityHashMap();
		        this.expression = Algebra.clone(this.expression, map, null);
		
		        // XXX we may need a method to get rid of generated EmptySet, when translating an
		        // algebraic expression to a SPARQL string
		        for (final QueryModelNode node : this.deletions) {
		            this.expression = (TupleExpr) replaceNode(this.expression, map.get(node),
		                    new EmptySet());
		        }
		
		        for (final Map.Entry<TupleExpr, String> entry : this.assignments.entrySet()) {
		            final QueryModelNode node = map.get(entry.getKey());
		            final Var var = new Var(entry.getValue());
		            if (node instanceof StatementPattern) {
		                ((StatementPattern) node).setContextVar(var);
		                ((StatementPattern) node).setScope(Scope.NAMED_CONTEXTS);
		            } else if (node instanceof ZeroLengthPath) {
		                ((ZeroLengthPath) node).setContextVar(var);
		                ((ZeroLengthPath) node).setScope(Scope.NAMED_CONTEXTS);
		            }
		        }
		
		        for (final Map.Entry<TupleExpr, Collection<String>> entry : this.filtering.asMap()
		                .entrySet()) {
		            this.expression = (TupleExpr) Algebra.insertFilter(this.expression,
		                    (TupleExpr) map.get(entry.getKey()),
		                    excludeCondition(this.prefix, entry.getValue()));
		        }
		    }
		
		}

		/**
		 * The URI prefix of graphs not to be read.
		 * @uml.property  name="prefix"
		 * @uml.associationEnd  
		 */
        private final URIPrefix prefix;

        /** The expression to be rewritten, at the beginning, then the rewritten expression. */
        private TupleExpr expression;

        /** A map with scheduled assignments of context variables to patterns in the expression. */
        private Map<TupleExpr, String> assignments;

        /** A map with scheduled filtering of context variables to be added in the expression. */
        private Multimap<TupleExpr, String> filtering;

        /** A set of nodes to be deleted. */
        private Set<TupleExpr> deletions;

        /** A counter used to generate the names of new context variables. */
        private int counter;

        /**
         * Creates a new instance for the URI prefix supplied.
         * 
         * @param prefix
         *            the URI prefix of graphs not to be read
         */
        public ExcludeReadingGraphsVisitor(final URIPrefix prefix)
        {
            Preconditions.checkNotNull(prefix);
            this.prefix = prefix;
        }

        /**
         * Rewrites the expression specified, using the URI prefix provided at construction time.
         * 
         * @param expression
         *            the expression to rewrite
         * @return the rewritten expression, possibly the supplied expression itself
         */
        public TupleExpr process(final TupleExpr expression)
        {
            this.expression = expression;
            this.assignments = Maps.newHashMap();
            this.filtering = HashMultimap.create();
            this.deletions = Sets.newHashSet();
            this.counter = 0;

            this.expression.visit(this);

            return this.expression == null || isTrivial(this.expression, null) ? null
                    : (TupleExpr) this.expression;
        }

        /**
         * {@inheritDoc} Updates the data structures holding scheduled modifications to the
         * expression while visiting the expression tree. After the whole tree is visited, the
         * rewritten expression is generated if some modification was scheduled.
         */
        @Override
        protected void meetNode(final QueryModelNode node) throws RuntimeException
        {
            if (node instanceof StatementPattern) {
                scheduleModifications((TupleExpr) node, ((StatementPattern) node).getContextVar());
            } else if (node instanceof ZeroLengthPath) {
                scheduleModifications((TupleExpr) node, ((ZeroLengthPath) node).getContextVar());
            }

            super.meetNode(node);

            if (node == this.expression
                    && (!this.deletions.isEmpty() || !this.filtering.isEmpty())) {
                generateExpression();
            }
        }

        /**
         * Analyzes the supplied pattern node and the associated context variable, scheduling the
         * appropriate modifications to the algebraic expression.
         * 
         * @param node
         *            the pattern node, either a {@link StatementPattern} or a
         *            {@link ZeroLengthPath}
         * @param var
         *            the context variable associated to the node
         */
        private void scheduleModifications(final TupleExpr node, final Var var)
        {
            if (var == null) {
                final String newVarName = "__c" + this.counter++;
                this.assignments.put(node, newVarName);
                this.filtering.put(findGroupPatternRoot(node), newVarName);

            } else if (var.getValue() == null) {
                this.filtering.put(findGroupPatternRoot(node), var.getName());

            } else if (var.getValue() instanceof URI && this.prefix.matches(var.getValue())) {
                this.deletions.add(node);
            }
        }

        /**
         * Generates the rewritten algebraic expression, if some modification was scheduled.
         */
        private void generateExpression()
        {
            final IdentityHashMap<QueryModelNode, QueryModelNode> map = Maps.newIdentityHashMap();
            this.expression = Algebra.clone(this.expression, map, null);

            // XXX we may need a method to get rid of generated EmptySet, when translating an
            // algebraic expression to a SPARQL string
            for (final QueryModelNode node : this.deletions) {
                this.expression = (TupleExpr) replaceNode(this.expression, map.get(node),
                        new EmptySet());
            }

            for (final Map.Entry<TupleExpr, String> entry : this.assignments.entrySet()) {
                final QueryModelNode node = map.get(entry.getKey());
                final Var var = new Var(entry.getValue());
                if (node instanceof StatementPattern) {
                    ((StatementPattern) node).setContextVar(var);
                    ((StatementPattern) node).setScope(Scope.NAMED_CONTEXTS);
                } else if (node instanceof ZeroLengthPath) {
                    ((ZeroLengthPath) node).setContextVar(var);
                    ((ZeroLengthPath) node).setScope(Scope.NAMED_CONTEXTS);
                }
            }

            for (final Map.Entry<TupleExpr, Collection<String>> entry : this.filtering.asMap()
                    .entrySet()) {
                this.expression = (TupleExpr) Algebra.insertFilter(this.expression,
                        (TupleExpr) map.get(entry.getKey()),
                        excludeCondition(this.prefix, entry.getValue()));
            }
        }

    }

}
