package eu.fbk.dkm.internal.util;

import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.RDFHandlerWrapper;

public final class RDFHandlers
{

    private RDFHandlers()
    {
        // empty
    }

    public static RDFHandler filter(final RDFHandler handler, final boolean enableStartRDF,
            final boolean enableHandleNamespace, final boolean enableHandleStatement,
            final boolean enableEndRDF)
    {
        return new AbstractRDFHandler() {

            @Override
            public void startRDF() throws RDFHandlerException
            {
                if (enableStartRDF) {
                    handler.startRDF();
                }
            }

            @Override
            public void handleNamespace(final String prefix, final String uri)
                    throws RDFHandlerException
            {
                if (enableHandleNamespace) {
                    handler.handleNamespace(prefix, uri);
                }
            }

            @Override
            public void handleStatement(final Statement statement) throws RDFHandlerException
            {
                if (enableHandleStatement) {
                    handler.handleStatement(statement);
                }
            }

            @Override
            public void endRDF() throws RDFHandlerException
            {
                if (enableEndRDF) {
                    handler.endRDF();
                }
            }

        };
    }

    public static RDFHandler filter(final RDFHandler handler, final boolean enableStartRDF,
            final Predicate<? super Namespace> namespacePredicate,
            final Predicate<? super Statement> statementPredicate, final boolean enableEndRDF)
    {
        return new AbstractRDFHandler() {

            @Override
            public void startRDF() throws RDFHandlerException
            {
                if (enableStartRDF) {
                    handler.startRDF();
                }
            }

            @Override
            public void handleNamespace(final String prefix, final String uri)
                    throws RDFHandlerException
            {
                final Namespace namespace = new SimpleNamespace(prefix, uri);
                if (namespacePredicate == null || namespacePredicate.apply(namespace)) {
                    handler.handleNamespace(prefix, uri);
                }
            }

            @Override
            public void handleStatement(final Statement statement) throws RDFHandlerException
            {
                if (statementPredicate == null || statementPredicate.apply(statement)) {
                    handler.handleStatement(statement);
                }
            }

            @Override
            public void endRDF() throws RDFHandlerException
            {
                if (enableEndRDF) {
                    handler.endRDF();
                }
            }

        };
    }

    public static RDFHandler transform(final RDFHandler handler,
            final Function<? super Statement, ? extends Iterable<? extends Statement>> //
            statementFunction,
            final Function<? super Namespace, ? extends Namespace> namespaceFunction)
    {
        return new RDFHandlerWrapper(handler) {

            @Override
            public void handleNamespace(final String prefix, final String uri)
                    throws RDFHandlerException
            {
                if (namespaceFunction != null) {
                    Namespace namespace = new SimpleNamespace(prefix, uri);
                    namespace = namespaceFunction.apply(namespace);
                    if (namespace != null) {
                        super.handleNamespace(namespace.getPrefix(), namespace.getName());
                    }

                } else {
                    super.handleNamespace(prefix, uri);
                }
            }

            @Override
            public void handleStatement(final Statement statement) throws RDFHandlerException
            {
                if (statementFunction != null) {

                    final Iterable<? extends Statement> transformedStatements;
                    transformedStatements = statementFunction.apply(statement);

                    if (transformedStatements != null) {
                        for (final Statement transformedStatement : transformedStatements) {
                            super.handleStatement(transformedStatement);
                        }
                    }

                } else {
                    super.handleStatement(statement);
                }
            }

        };
    }

    public static RDFHandler setContexts(final RDFHandler handler, final Resource... contexts)
    {
        if (contexts.length == 0) {
            return handler;

        } else if (contexts.length == 1) {
            final Resource context = contexts[0];
            return new AbstractRDFHandler() {

                @Override
                public void handleStatement(final Statement statement) throws RDFHandlerException
                {
                    handler.handleStatement(SimpleValueFactory.getInstance().createStatement(
                            statement.getSubject(), statement.getPredicate(),
                            statement.getObject(), context));
                }

            };

        } else {
            return new AbstractRDFHandler() {

                @Override
                public void handleStatement(final Statement statement) throws RDFHandlerException
                {
                    for (final Resource context : contexts) {
                        handler.handleStatement(SimpleValueFactory.getInstance().createStatement(
                                statement.getSubject(), statement.getPredicate(),
                                statement.getObject(), context));
                    }
                }

            };
        }
    }

    public static TupleQueryResultHandler asTupleQueryResultHandler(final RDFHandler handler,
            final Map<String, String> namespaces)
    {
        return new TupleQueryResultHandler() {

            @Override
            public void startQueryResult(final List<String> bindingNames)
                    throws TupleQueryResultHandlerException
            {
                try {
                    handler.startRDF();
                    for (final Map.Entry<String, String> entry : namespaces.entrySet()) {
                        handler.handleNamespace(entry.getKey(), entry.getValue());
                    }
                } catch (final RDFHandlerException ex) {
                    throw new TupleQueryResultHandlerException(ex);
                }
            }

            @Override
            public void handleSolution(final BindingSet bindings)
                    throws TupleQueryResultHandlerException
            {
                try {
                    handler.startRDF();
                    final Value subj = bindings.getValue("subject");
                    final Value pred = bindings.getValue("predicate");
                    final Value obj = bindings.getValue("object");
                    final Value context = bindings.getValue("context");
                    if (subj instanceof Resource && pred instanceof IRI) {
                        if (context == null) {
                            handler.handleStatement(SimpleValueFactory.getInstance()
                                    .createStatement((Resource) subj, (IRI) pred, obj));
                        } else if (context instanceof Resource) {
                            handler.handleStatement(
                                    SimpleValueFactory.getInstance().createStatement(
                                            (Resource) subj, (IRI) pred, obj, (Resource) context));
                        }
                    }
                } catch (final RDFHandlerException ex) {
                    throw new TupleQueryResultHandlerException(ex);
                }
            }

            @Override
            public void endQueryResult() throws TupleQueryResultHandlerException
            {
                try {
                    handler.endRDF();
                } catch (final RDFHandlerException ex) {
                    throw new TupleQueryResultHandlerException(ex);
                }
            }

            @Override
            public void handleBoolean(final boolean arg0) throws QueryResultHandlerException
            {
                // TODO Auto-generated method stub

            }

            @Override
            public void handleLinks(final List<String> arg0) throws QueryResultHandlerException
            {
                // TODO Auto-generated method stub

            }

        };
    }

}
