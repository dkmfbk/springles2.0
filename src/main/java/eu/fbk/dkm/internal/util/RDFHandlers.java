package eu.fbk.dkm.internal.util;

import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.helpers.RDFHandlerWrapper;

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
        return new RDFHandlerBase() {

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
        return new RDFHandlerBase() {

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
                final Namespace namespace = new NamespaceImpl(prefix, uri);
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

    public static RDFHandler transform(
            final RDFHandler handler,
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
                    Namespace namespace = new NamespaceImpl(prefix, uri);
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
            return new RDFHandlerBase() {

                @Override
                public void handleStatement(final Statement statement) throws RDFHandlerException
                {
                    handler.handleStatement(new ContextStatementImpl(statement.getSubject(),
                            statement.getPredicate(), statement.getObject(), context));
                }

            };

        } else {
            return new RDFHandlerBase() {

                @Override
                public void handleStatement(final Statement statement) throws RDFHandlerException
                {
                    for (final Resource context : contexts) {
                        handler.handleStatement(new ContextStatementImpl(statement.getSubject(),
                                statement.getPredicate(), statement.getObject(), context));
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
                    if (subj instanceof Resource && pred instanceof URI) {
                        if (context == null) {
                            handler.handleStatement(new StatementImpl((Resource) subj, (URI) pred,
                                    obj));
                        } else if (context instanceof Resource) {
                            handler.handleStatement(new ContextStatementImpl((Resource) subj,
                                    (URI) pred, obj, (Resource) context));
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
			public void handleBoolean(boolean arg0)
					throws QueryResultHandlerException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void handleLinks(List<String> arg0)
					throws QueryResultHandlerException {
				// TODO Auto-generated method stub
				
			}

        };
    }

}
