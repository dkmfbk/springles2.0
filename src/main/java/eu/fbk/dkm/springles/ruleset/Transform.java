package eu.fbk.dkm.springles.ruleset;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URLEncoder;
import java.util.ArrayDeque;
import java.util.Calendar;
import java.util.Date;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.ListBindingSet;

public final class Transform
{

    public static TupleQueryResult transform(final TupleQueryResult iteration,
            final String transformerURI, final Value... arguments) throws QueryEvaluationException
    {
        final Class<?> methodClass;
        String methodName;

        if (transformerURI.startsWith("java:")) {
            final int index = transformerURI.lastIndexOf('.');
            methodName = transformerURI.substring(index + 1);
            try {
                methodClass = Class.forName(transformerURI.substring(5, index));
            } catch (final ClassNotFoundException ex) {
                throw new IllegalArgumentException("No such transformer (invalid class name): "
                        + transformerURI);
            }
        } else {
            methodClass = Transform.class;
            methodName = new URIImpl(transformerURI).getLocalName();
        }

        Method method = null;
        final int numArgs = arguments.length + 1;
        for (final Method candidate : methodClass.getMethods()) {
            if (candidate.getName().equals(methodName)
                    && Modifier.isStatic(candidate.getModifiers())
                    && candidate.getParameterTypes().length == numArgs
                    && candidate.getParameterTypes()[0].isAssignableFrom(TupleQueryResult.class)
                    && TupleQueryResult.class.isAssignableFrom(candidate.getReturnType())) {
                method = candidate;
                break;
            }
        }
        if (method == null) {
            throw new IllegalArgumentException(
                    "No such transformer (invalid method or signature): " + transformerURI);
        }

        final Object[] args = new Object[numArgs];
        args[0] = iteration;
        for (int i = 1; i < numArgs; ++i) {
            final Class<?> type = method.getParameterTypes()[i];
            final Value arg = arguments[i - 1];
            if (arg == null || Value.class.isAssignableFrom(type)) {
                args[i] = arg;
            } else if (type == String.class) {
                args[i] = arg.stringValue();
            } else if (type == Boolean.class || type == boolean.class) {
                args[i] = ((Literal) arg).booleanValue();
            } else if (type == Character.class || type == char.class) {
                args[i] = ((Literal) arg).stringValue().charAt(0);
            } else if (type == Byte.class || type == byte.class) {
                args[i] = ((Literal) arg).byteValue();
            } else if (type == Short.class || type == short.class) {
                args[i] = ((Literal) arg).shortValue();
            } else if (type == Integer.class || type == int.class) {
                args[i] = ((Literal) arg).intValue();
            } else if (type == Long.class || type == long.class) {
                args[i] = ((Literal) arg).longValue();
            } else if (type == Float.class || type == float.class) {
                args[i] = ((Literal) arg).floatValue();
            } else if (type == Double.class || type == double.class) {
                args[i] = ((Literal) arg).doubleValue();
            } else if (type == Calendar.class) {
                args[i] = ((Literal) arg).calendarValue().toGregorianCalendar();
            } else if (type == Date.class) {
                args[i] = ((Literal) arg).calendarValue().toGregorianCalendar().getTime();
            } else {
                throw new IllegalArgumentException("Unsupported type: " + type.getName());
            }
        }

        try {
            return (TupleQueryResult) method.invoke(null, args);
        } catch (final IllegalAccessException ex) {
            // should not happen, as we searched for a public static method.
            throw new Error("Unexpected exception: " + ex.getMessage(), ex);
        } catch (final InvocationTargetException ex) {
            throw new QueryEvaluationException(ex.getCause().getMessage(), ex.getCause());
        }
    }

    public static TupleQueryResult tarjan(final TupleQueryResult iteration, final String prefix)
            throws QueryEvaluationException
    {
        return new TarjanTransformer(iteration, prefix);
    }

    private Transform()
    {
    }

    private static class TarjanTransformer extends AbstractTransformer
    {

        private static final List<String> NAMES = ImmutableList.of("comp", "node");

        private final Map<Resource, Node> nodes;

        private final String prefix;

        private final Deque<Node> stack;

        private int index;

        private final Iterator<Node> iterator;

        public TarjanTransformer(final TupleQueryResult iteration, final String prefix)
                throws QueryEvaluationException
        {
            super(NAMES, iteration);

            this.prefix = prefix;
            this.nodes = Maps.newHashMap();
            this.stack = new ArrayDeque<Transform.TarjanTransformer.Node>();
            this.index = 1;

            while (iteration.hasNext()) {
                final BindingSet bindings = iteration.next();
                final Node src = nodeFor(bindings.getValue("src"));
                final Node dest = nodeFor(bindings.getValue("dest"));
                if (src != null && dest != null) {
                    src.successors.add(dest);
                }
            }

            for (final Node node : this.nodes.values()) {
                if (node.index == 0) {
                    connect(node);
                }
            }

            this.iterator = this.nodes.values().iterator();
        }

        private Node nodeFor(@Nullable final Value value)
        {
            if (!(value instanceof Resource)) {
                return null;
            }
            Node node = this.nodes.get(value);
            if (node == null) {
                node = new Node((Resource) value);
                this.nodes.put((Resource) value, node);
            }
            return node;
        }

        private URI uriFor(final Node node)
        {
            try {
                return new URIImpl(this.prefix
                        + URLEncoder.encode(node.resource.stringValue(), "UTF-8"));
            } catch (final UnsupportedEncodingException ex) {
                throw new Error("Unexpected exception: " + ex.getMessage(), ex);
            }
        }

        private void connect(final Node node)
        {
            node.index = this.index;
            node.lowlink = this.index;
            ++this.index;
            this.stack.push(node);

            for (final Node successor : node.successors) {
                if (successor.index == 0) {
                    connect(successor);
                    node.lowlink = Math.min(node.lowlink, successor.lowlink);
                } else if (successor.component == null) {
                    node.lowlink = Math.min(node.lowlink, successor.lowlink);
                }
            }

            if (node.lowlink == node.index) {
                final URI uri = uriFor(node);
                Node element;
                do {
                    element = this.stack.pop();
                    element.component = uri;
                } while (element != node);
            }
        }

        @Override
        public boolean hasNext() throws QueryEvaluationException
        {
            return this.iterator.hasNext();
        }

        @Override
        public BindingSet next() throws QueryEvaluationException
        {
            final Node node = this.iterator.next();
            return new ListBindingSet(NAMES, new Value[] { node.component, node.resource });
        }

        private static final class Node
        {

            private final Resource resource;

            private final Set<Node> successors;

            private int index;

            private int lowlink;

            private URI component;

            private Node(final Resource resource)
            {
                this.resource = resource;
                this.successors = Sets.newHashSet();
            }

            @Override
            public String toString()
            {
                return this.resource.stringValue();
            }

        }

    }

    private abstract static class AbstractTransformer implements TupleQueryResult
    {

        private final List<String> bindingNames;

        private final TupleQueryResult iteration;

        protected AbstractTransformer(final List<String> bindingNames,
                final TupleQueryResult iteration)
        {
            this.bindingNames = bindingNames;
            this.iteration = iteration;
        }

        @Override
        public final List<String> getBindingNames()
        {
            return this.bindingNames;
        }

        @Override
        public final void remove() throws QueryEvaluationException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public final void close() throws QueryEvaluationException
        {
            this.iteration.close();
        }

    }

}
