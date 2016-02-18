package eu.fbk.dkm.internal.util;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import org.openrdf.model.Literal;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.ListBindingSet;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.ExceptionConvertingIteration;
import info.aduna.iteration.FilterIteration;
import info.aduna.iteration.Iteration;

public final class Iterations
{

    private static final Logger LOGGER = LoggerFactory.getLogger(Iterations.class);

    // CONVERSION AND TRANSFORMATION

    public static <T> CloseableIteration<T, RepositoryException> asRepositoryIteration(
            final Iteration<T, ? extends Exception> result)
    {
        Preconditions.checkNotNull(result);

        return new ExceptionConvertingIteration<T, RepositoryException>(result) {

            @Override
            protected RepositoryException convert(final Exception ex)
            {
                return ex instanceof RepositoryException ? (RepositoryException) ex
                        : new RepositoryException(ex.getMessage(), ex);
            }

        };
    }

    public static GraphQueryResult asGraphQueryResult(
            final Iteration<? extends Statement, ? extends Exception> iteration,
            final Map<String, String> namespaces)
    {
        return new GraphQueryResultAdapter<Statement>(iteration, namespaces) {

            @Override
            protected Statement convert(final Statement statement) throws Exception
            {
                return statement;
            }

        };
    }

    public static GraphQueryResult asGraphQueryResult(
            final Iteration<? extends BindingSet, ? extends Exception> iteration,
            final Map<String, String> namespaces, final ValueFactory valueFactory)
    {
        return new GraphQueryResultAdapter<BindingSet>(iteration, namespaces) {

            @Override
            protected Statement convert(final BindingSet bindings) throws Exception
            {
                final Value subj = bindings.getValue("subject");
                final Value pred = bindings.getValue("predicate");
                final Value obj = bindings.getValue("object");
                final Value context = bindings.getValue("context");
                if (!(subj instanceof Resource) || !(pred instanceof URI) || obj == null
                        || context != null && !(context instanceof Resource)) {
                    return null;
                } else if (context == null) {
                    return valueFactory.createStatement((Resource) subj, (URI) pred, obj);
                } else {
                    return valueFactory.createStatement((Resource) subj, (URI) pred, obj,
                            (Resource) context);
                }
            }

        };
    }

    public static TupleQueryResult asTupleQueryResult(
            final Iteration<? extends Statement, ? extends Exception> iteration)
    {
        final List<String> bindingNames = ImmutableList.of("subject", "predicate", "object",
                "context");
        return new TupleQueryResultAdapter<Statement>(bindingNames, iteration) {

            @Override
            protected BindingSet convert(final Statement statement) throws Exception
            {
                return new ListBindingSet(bindingNames, statement.getSubject(),
                        statement.getPredicate(), statement.getObject(), statement.getContext());
            }

        };
    }

    public static <T extends Value> TupleQueryResult asTupleQueryResult(
            final Iteration<T, ? extends Exception> iteration, final String bindingName)
    {
        final List<String> bindingNames = ImmutableList.of(bindingName);
        return new TupleQueryResultAdapter<Value>(bindingNames, iteration) {

            @Override
            protected Value[] convertToValues(final Value element) throws Exception
            {
                return new Value[] { element };
            }

        };
    }

    public static TupleQueryResult asTupleQueryResult(
            final Iteration<? extends Namespace, ? extends Exception> iteration,
            final String prefixBindingName, final String nameBindingName)
    {
        final List<String> bindingNames = ImmutableList.of(prefixBindingName, nameBindingName);
        return new TupleQueryResultAdapter<Namespace>(bindingNames, iteration) {

            @Override
            protected Value[] convertToValues(final Namespace namespace) throws Exception
            {
                return new Value[] { new LiteralImpl(namespace.getPrefix(), XMLSchema.STRING),
                        new LiteralImpl(namespace.getName(), XMLSchema.STRING) };
            }

        };
    }

    public static <E extends Exception> CloseableIteration<Namespace, E> asNamespaceIteration(
            final Iteration<? extends BindingSet, E> iteration, final String prefixBindingName,
            final String nameBindingName) throws E
    {
        return new CloseableIteration<Namespace, E>() {

            private Namespace next = null;

            @Override
            public boolean hasNext() throws E
            {
                if (this.next != null) {
                    return true;

                } else {
                    while (iteration.hasNext()) {
                        final BindingSet bindings = iteration.next();
                        final Value prefix = bindings.getValue(prefixBindingName);
                        final Value name = bindings.getValue(nameBindingName);
                        if (prefix instanceof Literal && name instanceof Literal) {
                            this.next = new NamespaceImpl(prefix.stringValue(), name.stringValue());
                        }
                    }
                    return false;
                }
            }

            @Override
            public Namespace next() throws E
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                final Namespace result = this.next;
                this.next = null;
                return result;
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() throws E
            {
                Iterations.close(iteration);
            }

        };
    }

    public static <T> Iterator<T> asIterator(final Iteration<T, ?> iteration)
    {
        Preconditions.checkNotNull(iteration);

        return new Iterator<T>() {

            @Override
            public boolean hasNext()
            {
                try {
                    return iteration.hasNext();
                } catch (final Exception ex) {
                    throw new UndeclaredThrowableException(ex);
                }
            }

            @Override
            public T next()
            {
                try {
                    return iteration.next();
                } catch (final Exception ex) {
                    throw new UndeclaredThrowableException(ex);
                }

            }

            @Override
            public void remove()
            {
                try {
                    iteration.remove();
                } catch (final Exception ex) {
                    throw new UndeclaredThrowableException(ex);
                }
            }

        };
    }

    public static <T> Iterable<T> asIterable(final Iteration<T, ?> iteration)
    {
        Preconditions.checkNotNull(iteration);

        return new Iterable<T>() {

            @Override
            public Iterator<T> iterator()
            {
                return asIterator(iteration);
            }

        };
    }

    public static <T, E extends Exception> CloseableIteration<T, E> filter(
            final Iteration<T, E> iteration, final Predicate<? super T> retainPredicate)
    {
        return new FilterIteration<T, E>(iteration) {

            @Override
            protected boolean accept(final T element) throws E
            {
                return retainPredicate.apply(element);
            }

        };
    }

    public static <T, E extends Exception> CloseableIteration<T, E> project(
            final Iteration<? extends BindingSet, E> iteration, final String bindingName,
            final Class<T> valueClass)
    {
        return new CloseableIteration<T, E>() {

            private T next = null;

            @Override
            public boolean hasNext() throws E
            {
                if (this.next != null) {
                    return true;

                } else {
                    while (iteration.hasNext()) {
                        final Value value = iteration.next().getValue(bindingName);
                        if (valueClass.isInstance(value)) {
                            this.next = valueClass.cast(value);
                            return true;
                        }
                    }
                    return false;
                }
            }

            @Override
            public T next() throws E
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                final T result = this.next;
                this.next = null;
                return result;
            }

            @Override
            public void remove() throws E
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() throws E
            {
                Iterations.close(iteration);
            }

        };
    }

    // ELEMENT EXTRACTION

    public static <T, E extends Exception> T getFirst(final Iteration<T, E> iteration,
            @Nullable final T defaultValue) throws E
    {
        try {
            return iteration.hasNext() ? iteration.next() : defaultValue;
        } finally {
            if (iteration instanceof CloseableIteration<?, ?>) {
                ((CloseableIteration<T, E>) iteration).close();
            }
        }
    }

    public static <T, E extends Exception> T getOnlyElement(final Iteration<T, E> iteration)
            throws E
    {
        try {
            final T result = iteration.next();
            Preconditions.checkArgument(!iteration.hasNext());
            return result;

        } finally {
            if (iteration instanceof CloseableIteration<?, ?>) {
                ((CloseableIteration<T, E>) iteration).close();
            }
        }
    }

    public static <T, E extends Exception> List<T> getAllElements(
            final Iteration<? extends T, E> iteration) throws E
    {
        final ImmutableList.Builder<T> builder = ImmutableList.builder();
        try {
            while (iteration.hasNext()) {
                builder.add(iteration.next());
            }
            return builder.build();

        } finally {
            Iterations.close(iteration);
        }

    }

    // CLOSING

    public static void closeQuietly(final Iteration<?, ?> iteration)
    {
        if (iteration != null) {
            try {
                if (iteration instanceof CloseableIteration<?, ?>) {
                    ((CloseableIteration<?, ?>) iteration).close();
                }
            } catch (final Throwable ex) {
                LOGGER.warn("Unexpected error caught while closing iteration.", ex);
            }
        }
    }

    public static <E extends Exception> void close(final Iteration<?, E> iteration) throws E
    {
        if (iteration instanceof CloseableIteration<?, ?>) {
            ((CloseableIteration<?, E>) iteration).close();
        }
    }

    private Iterations()
    {
    }

    private abstract static class ResultAdapter<R, T> implements
            CloseableIteration<R, QueryEvaluationException>
    {

        private final Iteration<? extends T, ? extends Exception> iteration;

        private R next;

        protected ResultAdapter(final Iteration<? extends T, ? extends Exception> iteration)
        {
            this.iteration = iteration;
            this.next = null;
        }

        @Override
        public final boolean hasNext() throws QueryEvaluationException
        {
            try {
                while (this.next == null && this.iteration.hasNext()) {
                    this.next = convert(this.iteration.next());
                }
                return this.next != null;

            } catch (final RuntimeException ex) {
                throw ex;
            } catch (final Exception ex) {
                throw ex instanceof QueryEvaluationException ? (QueryEvaluationException) ex
                        : new QueryEvaluationException(ex);
            }
        }

        @Override
        public final R next() throws QueryEvaluationException
        {
            try {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                final R result = this.next;
                this.next = null;
                return result;

            } catch (final RuntimeException ex) {
                throw ex;
            } catch (final Exception ex) {
                throw ex instanceof QueryEvaluationException ? (QueryEvaluationException) ex
                        : new QueryEvaluationException(ex);
            }
        }

        @Override
        public final void remove() throws QueryEvaluationException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public final void close() throws QueryEvaluationException
        {
            try {
                Iterations.close(this.iteration);

            } catch (final RuntimeException ex) {
                throw ex;
            } catch (final Exception ex) {
                throw ex instanceof QueryEvaluationException ? (QueryEvaluationException) ex
                        : new QueryEvaluationException(ex);
            }
        }

        protected abstract R convert(T element) throws Exception;

    }

    private abstract static class TupleQueryResultAdapter<T> extends ResultAdapter<BindingSet, T>
            implements TupleQueryResult
    {

        private final List<String> bindingNames;

        public TupleQueryResultAdapter(final List<String> bindingNames,
                final Iteration<? extends T, ? extends Exception> iteration)
        {
            super(iteration);
            this.bindingNames = ImmutableList.copyOf(bindingNames);
        }

        @Override
        public List<String> getBindingNames()
        {
            return this.bindingNames;
        }

        @Override
        protected BindingSet convert(final T element) throws Exception
        {
            return new ListBindingSet(this.bindingNames, convertToValues(element));
        }

        protected Value[] convertToValues(final T element) throws Exception
        {
            return null;
        }

    }

    private abstract static class GraphQueryResultAdapter<T> extends ResultAdapter<Statement, T>
            implements GraphQueryResult
    {

        private final Map<String, String> namespaces;

        public GraphQueryResultAdapter(
                final Iteration<? extends T, ? extends Exception> iteration,
                final Map<String, String> namespaces)
        {
            super(iteration);
            this.namespaces = namespaces;
        }

        @Override
        public Map<String, String> getNamespaces()
        {
            return this.namespaces;
        }

    }

}
