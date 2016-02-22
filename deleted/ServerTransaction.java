package eu.fbk.dkm.springles.store;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResultUtil;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;

import eu.fbk.dkm.internal.springles.protocol.Request;
import eu.fbk.dkm.internal.springles.protocol.Request.Argument;
import eu.fbk.dkm.internal.springles.protocol.Request.Command;
import eu.fbk.dkm.internal.springles.protocol.Response;
import eu.fbk.dkm.internal.springles.protocol.Response.ResponseType;
import eu.fbk.dkm.internal.springles.protocol.Settings;
import eu.fbk.dkm.springles.SpringlesRepository;
import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.dkm.springles.base.ForwardingTransaction;
import eu.fbk.dkm.springles.base.QuerySpec;
import eu.fbk.dkm.springles.base.QueryType;
import eu.fbk.dkm.springles.base.Transaction;
import eu.fbk.dkm.springles.base.UpdateSpec;

final class ServerTransaction extends ForwardingTransaction
{

    private final Transaction delegate;

    private final Supplier<Settings> settingsSupplier;

    public ServerTransaction(final Transaction delegate, final Supplier<Settings> settingsSupplier)
    {
        Preconditions.checkNotNull(delegate);
        Preconditions.checkNotNull(settingsSupplier);

        this.delegate = delegate;
        this.settingsSupplier = settingsSupplier;
    }

    @Override
    protected Transaction delegate()
    {
        return this.delegate;
    }

    @Override
    public void query(final QuerySpec<?> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode, final int timeout,
            final Object handler) throws QueryEvaluationException, RepositoryException
    {
        if (!SpringlesRepository.PROTOCOL.equals(query.getLanguage())) {
            delegate().query(query, dataset, bindings, mode, timeout, handler);

        } else {
            if (!QueryType.TUPLE.equals(query.getType())) {
                throw new IllegalArgumentException(
                        "Commands have to be encapsulated in tuple queries.");
            }
            try {
                QueryResultUtil.report(
                        (TupleQueryResult) query(query, dataset, bindings, mode, timeout),
                        (TupleQueryResultHandler) handler);
            } catch (final TupleQueryResultHandlerException ex) {
                throw new QueryEvaluationException(ex);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T query(final QuerySpec<T> query, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode, final int timeout)
            throws QueryEvaluationException, RepositoryException
    {
        if (!SpringlesRepository.PROTOCOL.equals(query.getLanguage())) {
            return delegate().query(query, dataset, bindings, mode, timeout);

        } else {
            if (!QueryType.TUPLE.equals(query.getType())) {
                throw new IllegalArgumentException(
                        "Commands have to be encapsulated in tuple queries.");
            }
            return (T) executeRead(Request.fromXML(query.getString())).getContent();
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Response executeRead(final Request request) throws QueryEvaluationException,
            RepositoryException
    {
        final Command command = request.getCommand();

        if (command == Command.CONNECT) {
            return Response.create(ResponseType.SETTINGS, //
                    this.settingsSupplier.get());

        } else if (command == Command.GET_NAMESPACES) {
            return Response.create(ResponseType.NAMESPACES, //
                    delegate().getNamespaces());

        } else if (command == Command.GET_CONTEXT_IDS) {
            return Response.create(ResponseType.RESOURCES, //
                    delegate().getContextIDs(//
                            request.getArg(Argument.INFERENCE_MODE)));

        } else if (command == Command.GET_STATEMENTS) {
            return Response.create(ResponseType.STATEMENTS, //
                    delegate().getStatements(//
                            request.getArg(Argument.SUBJECT), //
                            request.getArg(Argument.PREDICATE), //
                            request.getArg(Argument.OBJECT), //
                            request.getArg(Argument.INFERENCE_MODE), //
                            request.getArg(Argument.CONTEXTS)));

        } else if (command == Command.HAS_STATEMENT) {
            return Response.create(ResponseType.BOOLEAN, //
                    delegate().hasStatement(//
                            request.getArg(Argument.SUBJECT), //
                            request.getArg(Argument.PREDICATE), //
                            request.getArg(Argument.OBJECT), //
                            request.getArg(Argument.INFERENCE_MODE), //
                            request.getArg(Argument.CONTEXTS)));

        } else if (command == Command.SIZE) {
            return Response.create(ResponseType.LONG, //
                    delegate().size(//
                            request.getArg(Argument.INFERENCE_MODE), //
                            request.getArg(Argument.CONTEXTS)));

        } else if (command == Command.GET_CLOSURE_STATUS) {
            return Response.create(ResponseType.CLOSURE_STATUS, //
                    delegate().getClosureStatus());

        } else if (command == Command.QUERY) {
            try {
                final QueryType<?> queryType = QueryType.valueOf(request
                        .getArg(Argument.QUERY_TYPE));
                final Object result = delegate().query(//
                        QuerySpec.from(//
                                queryType, //
                                request.getArg(Argument.QUERY_STRING), //
                                request.getArg(Argument.LANGUAGE), //
                                request.getArg(Argument.BASE_URI)), //
                        request.getArg(Argument.DATASET), //
                        request.getArg(Argument.BINDINGS), //
                        request.getArg(Argument.INFERENCE_MODE), //
                        request.getArg(Argument.TIMEOUT));
                return Response.create(
                        (ResponseType) ResponseType.forQueryClass(queryType.getQueryClass()),
                        result);
            } catch (final MalformedQueryException ex) {
                throw new RepositoryException(
                        "Malformed query string (should have been checked on client side)", ex);
            }

        } else if (command == Command.QUERY_NAMED) {
            final QueryType<?> queryType = QueryType.valueOf(request.getArg(Argument.QUERY_TYPE));
            final Object result = delegate().query(//
                    request.getArg(Argument.QUERY_URI), //
                    queryType, //
                    request.getArg(Argument.INFERENCE_MODE), //
                    request.getArg(Argument.PARAMETERS));
            return Response.create(
                    (ResponseType) ResponseType.forQueryClass(queryType.getQueryClass()), result);

        } else if (command == Command.END) {
            delegate().end(//
                    request.getArg(Argument.COMMIT));
            return Response.create(ResponseType.BOOLEAN, Boolean.TRUE);

        } else {
            throw new IllegalArgumentException("Invalid read command: " + command);
        }
    }

    @Override
    public void update(final UpdateSpec update, @Nullable final Dataset dataset,
            @Nullable final BindingSet bindings, final InferenceMode mode)
            throws UpdateExecutionException, RepositoryException
    {
        if (!SpringlesRepository.PROTOCOL.equals(update.getLanguage())) {
            delegate().update(update, dataset, bindings, mode);
        } else {
            executeWrite(Request.fromXML(update.getString()));
        }
    }

    private void executeWrite(final Request request) throws UpdateExecutionException,
            RepositoryException
    {
        final Command command = request.getCommand();

        if (command == Command.SET_NAMESPACE) {
            delegate().setNamespace(//
                    request.getArg(Argument.PREFIX), //
                    request.getArg(Argument.NAME));

        } else if (command == Command.CLEAR_NAMESPACES) {
            delegate().clearNamespaces();

        } else if (command == Command.ADD) {
            delegate().add(//
                    request.getArg(Argument.STATEMENTS), //
                    request.getArg(Argument.CONTEXTS));

        } else if (command == Command.REMOVE) {
            delegate().remove(//
                    request.getArg(Argument.STATEMENTS), //
                    request.getArg(Argument.CONTEXTS));

        } else if (command == Command.REMOVE_MATCHING) {
            delegate().remove(//
                    request.getArg(Argument.SUBJECT), //
                    request.getArg(Argument.PREDICATE), //
                    request.getArg(Argument.OBJECT), //
                    request.getArg(Argument.CONTEXTS));

        } else if (command == Command.RESET) {
            delegate().reset();

        } else if (command == Command.UPDATE_CLOSURE) {
            delegate().updateClosure();

        } else if (command == Command.CLEAR_CLOSURE) {
            delegate().clearClosure();

        } else if (command == Command.END) {
            delegate().end(request.getArg(Argument.COMMIT));

        } else if (command == Command.UPDATE) {
            try {
                delegate().update(//
                        UpdateSpec.from(//
                                request.getArg(Argument.UPDATE_STRING), //
                                request.getArg(Argument.LANGUAGE), //
                                request.getArg(Argument.BASE_URI)), //
                        request.getArg(Argument.DATASET), //
                        request.getArg(Argument.BINDINGS), //
                        request.getArg(Argument.INFERENCE_MODE));
            } catch (final MalformedQueryException ex) {
                throw new RepositoryException(
                        "Malformed update string (should have been checked on client side)", ex);
            }

        } else if (command == Command.UPDATE_NAMED) {
            delegate().update(//
                    request.getArg(Argument.UPDATE_URI), //
                    request.getArg(Argument.INFERENCE_MODE), //
                    request.getArg(Argument.PARAMETERS));
        }
    }

}
