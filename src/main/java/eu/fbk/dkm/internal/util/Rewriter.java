package eu.fbk.dkm.internal.util;

import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.BindingSetAssignment;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UpdateExpr;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.SailConnectionWrapper;
import org.openrdf.sail.helpers.SailWrapper;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.ConvertingIteration;

public abstract class Rewriter {

    public static final Rewriter IDENTITY = new Rewriter() {

        @Nullable
        @Override
        public Value rewriteValue(final ValueFactory factory, final Value value) {
            return value;
        }

    };

    public static final Rewriter SKOLEMIZER = new Rewriter() {

        @Nullable
        @Override
        public Value rewriteValue(final ValueFactory factory, @Nullable final Value value) {
            if (!(value instanceof BNode)) {
                return value;
            }
            return factory.createURI("bnode:", ((BNode) value).getID());
        }

    };

    public static final Rewriter DESKOLEMIZER = new Rewriter() {

        @Nullable
        @Override
        public Value rewriteValue(final ValueFactory factory, @Nullable final Value value) {
            if (value instanceof URI) {
                final URI uri = (URI) value;
                if (uri.getNamespace().equals("bnode:")) {
                    return factory.createBNode(uri.getLocalName());
                }
            }
            return value;
        }

    };

    @Nullable
    public abstract Value rewriteValue(ValueFactory factory, @Nullable Value value);

    public final Resource[] rewriteContexts(final ValueFactory factory,
            @Nullable final Resource... contexts) {
        if (contexts == null) {
            return null;
        }

        Resource[] result = contexts;
        final int length = result.length;
        for (int i = 0; i < length; ++i) {
            final Resource oldContext = result[i];
            final Resource newContext = (Resource) rewriteValue(factory, oldContext);
            if (oldContext != newContext) {
                if (result == contexts) {
                    result = result.clone();
                }
                result[i] = newContext;
            }
        }
        return result;
    }

    @Nullable
    public final <E extends Exception> CloseableIteration<Resource, E> rewriteContexts(
            final ValueFactory factory,
            @Nullable final CloseableIteration<? extends Resource, ? extends E> iteration) {
        if (iteration == null) {
            return null;
        }

        return new ConvertingIteration<Resource, Resource, E>(iteration) {

            @Override
            protected Resource convert(final Resource context) throws E {
                return (Resource) Rewriter.this.rewriteValue(factory, context);
            }

        };
    }

    @Nullable
    public final Statement rewriteStatement(final ValueFactory factory,
            @Nullable final Statement statement) {
        if (statement == null) {
            return null;
        }

        final Resource oldSubj = statement.getSubject();
        final Value oldObj = statement.getObject();
        final Resource oldCtx = statement.getContext();

        final Resource newSubj = (Resource) rewriteValue(factory, oldSubj);
        final Value newObj = rewriteValue(factory, oldObj);
        final Resource newCtx = (Resource) rewriteValue(factory, oldCtx);

        if (oldSubj == newSubj && oldObj == newObj && oldCtx == newCtx) {
            return statement;
        }

        final URI pred = statement.getPredicate();
        if (newCtx == null) {
            return factory.createStatement(newSubj, pred, newObj);
        }
        return factory.createStatement(newSubj, pred, newObj, newCtx);
    }

    @Nullable
    public final Iterable<Statement> rewriteStatements(final ValueFactory factory,
            @Nullable final Iterable<? extends Statement> statements) {
        if (statements == null) {
            return null;
        }

        return Iterables.transform(statements, new Function<Statement, Statement>() {

            @Override
            @Nullable
            public Statement apply(@Nullable final Statement statement) {
                return Rewriter.this.rewriteStatement(factory, statement);
            }

        });
    }

    @Nullable
    public final <E extends Exception> CloseableIteration<Statement, E> rewriteStatements(
            final ValueFactory factory,
            @Nullable final CloseableIteration<? extends Statement, ? extends E> iteration) {
        if (iteration == null) {
            return null;
        }

        return new ConvertingIteration<Statement, Statement, E>(iteration) {

            @Override
            protected Statement convert(final Statement statement) throws E {
                return Rewriter.this.rewriteStatement(factory, statement);
            }

        };
    }

    @Nullable
    public final BindingSet rewriteBindings(final ValueFactory factory,
            @Nullable final BindingSet bindings) {
        if (bindings == null) {
            return null;
        }

        final int size = bindings.size();
        final Binding[] array = new Binding[size];
        boolean changed = false;
        int i = 0;
        for (final Binding binding : bindings) {
            final Value oldValue = binding.getValue();
            final Value newValue = rewriteValue(factory, oldValue);
            if (oldValue == newValue) {
                array[i] = binding;
            } else {
                array[i] = new BindingImpl(binding.getName(), newValue);
                changed = true;
            }
            ++i;
        }

        if (!changed) {
            return bindings;
        }

        final MapBindingSet result = new MapBindingSet(size);
        for (final Binding binding : array) {
            result.addBinding(binding);
        }
        return result;
    }

    @Nullable
    public final <E extends Exception> CloseableIteration<BindingSet, E> rewriteBindings(
            final ValueFactory factory,
            @Nullable final CloseableIteration<? extends BindingSet, ? extends E> iteration) {
        if (iteration == null) {
            return null;
        }

        return new ConvertingIteration<BindingSet, BindingSet, E>(iteration) {

            @Override
            protected BindingSet convert(final BindingSet bindings) throws E {
                return Rewriter.this.rewriteBindings(factory, bindings);
            }

        };
    }

    @Nullable
    public final Dataset rewriteDataset(final ValueFactory factory, //
            @Nullable final Dataset dataset) {
        if (dataset == null) {
            return null;
        }

        final DatasetImpl result = new DatasetImpl();
        boolean changed = false;

        for (final URI graph : dataset.getDefaultGraphs()) {
            final URI newGraph = (URI) rewriteValue(factory, graph);
            result.addDefaultGraph(newGraph);
            changed |= newGraph != graph;
        }

        for (final URI graph : dataset.getNamedGraphs()) {
            final URI newGraph = (URI) rewriteValue(factory, graph);
            result.addNamedGraph(newGraph);
            changed |= newGraph != graph;
        }

        for (final URI graph : dataset.getDefaultRemoveGraphs()) {
            final URI newGraph = (URI) rewriteValue(factory, graph);
            result.addDefaultRemoveGraph(newGraph);
            changed |= newGraph != graph;
        }

        final URI insertGraph = dataset.getDefaultInsertGraph();
        final URI newInsertGraph = (URI) rewriteValue(factory, insertGraph);
        result.setDefaultInsertGraph(newInsertGraph);
        changed |= newInsertGraph != insertGraph;

        return changed ? result : dataset;
    }

    @Nullable
    public final <T extends QueryModelNode> T rewriteExpr(final ValueFactory factory,
            @Nullable final T expr) {
        if (expr == null) {
            return null;
        }

        @SuppressWarnings("unchecked")
        final T result = (T) expr.clone();

        result.visit(new QueryModelVisitorBase<RuntimeException>() {

            @Override
            public void meet(final BindingSetAssignment node) throws RuntimeException {
                final List<BindingSet> bindingsList = Lists.newArrayList();
                for (final BindingSet bindings : node.getBindingSets()) {
                    bindingsList.add(Rewriter.this.rewriteBindings(factory, bindings));
                }
                node.setBindingSets(bindingsList);
            }

            @Override
            public void meet(final ValueConstant node) throws RuntimeException {
                node.setValue(Rewriter.this.rewriteValue(factory, node.getValue()));
            }

            @Override
            public void meet(final Var node) throws RuntimeException {
                node.setValue(Rewriter.this.rewriteValue(factory, node.getValue()));
            }

        });

        return result;
    }

    @Nullable
    public static Sail newSkolemizingSail(@Nullable final ValueFactory factory,
            @Nullable final Sail sail) {
        return Rewriter
                .newRewritingSail(factory, sail, Rewriter.SKOLEMIZER, Rewriter.DESKOLEMIZER);
    }

    public static SailConnection newSkolemizingSailConnection(final ValueFactory factory,
            @Nullable final SailConnection connection) {
        return Rewriter.newRewritingSailConnection(factory, connection, Rewriter.SKOLEMIZER,
                Rewriter.DESKOLEMIZER);
    }

    @Nullable
    public static Sail newRewritingSail(@Nullable final ValueFactory factory,
            @Nullable final Sail sail, final Rewriter inRewriter, final Rewriter outRewriter) {
        if (sail == null) {
            return null;
        }

        return new SailWrapper(sail) {

            @Override
            public SailConnection getConnection() throws SailException {
                return Rewriter.newRewritingSailConnection(
                        Objects.firstNonNull(factory, super.getValueFactory()),
                        super.getConnection(), inRewriter, outRewriter);
            }

        };
    }

    @Nullable
    public static SailConnection newRewritingSailConnection(final ValueFactory factory,
            @Nullable final SailConnection connection, final Rewriter inRewriter,
            final Rewriter outRewriter) {
        if (connection == null) {
            return null;
        }

        return new SailConnectionWrapper(connection) {

            // Statement operations

            @Override
            public CloseableIteration<? extends Statement, SailException> getStatements(
                    final Resource subj, final URI pred, final Value obj,
                    final boolean includeInferred, final Resource... contexts)
                    throws SailException {

                final Resource newSubj = (Resource) inRewriter.rewriteValue(factory, subj);
                final URI newPred = (URI) inRewriter.rewriteValue(factory, pred);
                final Value newObj = inRewriter.rewriteValue(factory, obj);
                final Resource[] newContexts = inRewriter.rewriteContexts(factory, contexts);

                return outRewriter.rewriteStatements(factory, super.getStatements(newSubj,
                        newPred, newObj, includeInferred, newContexts));
            }

            @Override
            public void addStatement(final Resource subj, final URI pred, final Value obj,
                    final Resource... contexts) throws SailException {

                final Resource newSubj = (Resource) inRewriter.rewriteValue(factory, subj);
                final URI newPred = (URI) inRewriter.rewriteValue(factory, pred);
                final Value newObj = inRewriter.rewriteValue(factory, obj);
                final Resource[] newContexts = inRewriter.rewriteContexts(factory, contexts);

                super.addStatement(newSubj, newPred, newObj, newContexts);
            }

            @Override
            public void removeStatements(final Resource subj, final URI pred, final Value obj,
                    final Resource... contexts) throws SailException {

                final Resource newSubj = (Resource) inRewriter.rewriteValue(factory, subj);
                final URI newPred = (URI) inRewriter.rewriteValue(factory, pred);
                final Value newObj = inRewriter.rewriteValue(factory, obj);
                final Resource[] newContexts = inRewriter.rewriteContexts(factory, contexts);

                super.removeStatements(newSubj, newPred, newObj, newContexts);
            }

            // Context operations

            @Override
            public long size(final Resource context) throws SailException {
                final Resource newContext = (Resource) inRewriter.rewriteValue(factory, context);
                return super.size(newContext);
            }

            @Override
            public CloseableIteration<? extends Resource, SailException> getContextIDs()
                    throws SailException {
                return outRewriter.rewriteContexts(factory, super.getContextIDs());
            }

            @Override
            public void clear(final Resource... contexts) throws SailException {
                final Resource[] newContexts = inRewriter.rewriteContexts(factory, contexts);
                super.clear(newContexts);
            }

            // SPARQL operations

            @Override
            public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(
                    final TupleExpr expr, final Dataset dataset, final BindingSet bindings,
                    final boolean includeInferred) throws SailException {

                final TupleExpr newExpr = inRewriter.rewriteExpr(factory, expr);
                final Dataset newDataset = inRewriter.rewriteDataset(factory, dataset);
                final BindingSet newBindings = inRewriter.rewriteBindings(factory, bindings);

                return outRewriter.rewriteBindings(factory,
                        super.evaluate(newExpr, newDataset, newBindings, includeInferred));
            }

//            @Override
 //           public void executeUpdate(final UpdateExpr expr, final Dataset dataset,
  //                  final BindingSet bindings, final boolean includeInferred) throws SailException {

  //              final UpdateExpr newExpr = inRewriter.rewriteExpr(factory, expr);
  //              final Dataset newDataset = inRewriter.rewriteDataset(factory, dataset);
  //              final BindingSet newBindings = inRewriter.rewriteBindings(factory, bindings);

   //             super.executeUpdate(newExpr, newDataset, newBindings, includeInferred);
   //         }

        };
    }

}
