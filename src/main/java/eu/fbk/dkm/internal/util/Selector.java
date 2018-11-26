package eu.fbk.dkm.internal.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

public final class Selector
{

    private static final Resource[] ALL_CONTEXTS = new Resource[] {};

    private static final DatatypeFactory DATATYPE_FACTORY;

    static {
        try {
            DATATYPE_FACTORY = DatatypeFactory.newInstance();
        } catch (final Throwable ex) {
            throw new Error(ex);
        }
    }

    private final Model graph;

    private final Resource node;

    private final Resource[] contexts;

    protected Selector(final Model graph, final Resource node, final Resource[] contexts)
    {
        this.graph = graph;
        this.node = node;
        this.contexts = contexts;
    }

    // SELECTION

    public static Selector select(final Model graph, final Resource node)
    {
        Preconditions.checkNotNull(graph);
        Preconditions.checkNotNull(node);

        return new Selector(graph, node, Selector.ALL_CONTEXTS);
    }

    public static Selector select(final Model graph, final Resource node,
            final Resource... contexts)
    {
        Preconditions.checkNotNull(graph);
        Preconditions.checkNotNull(node);
        Preconditions.checkNotNull(contexts);

        return new Selector(graph, node, contexts);
    }

    public Selector select(final IRI predicate)
    {
        final Resource node = this.get(predicate, Resource.class);
        return this.node == node ? this : new Selector(this.graph, node, this.contexts);
    }

    public static Iterable<Selector> selectAll(final Model graph,
            final Iterable<? extends Resource> nodes, final Resource... contexts)
    {
        Preconditions.checkNotNull(graph);
        Preconditions.checkNotNull(nodes);
        Preconditions.checkNotNull(contexts);

        return new Iterable<Selector>() {

            @Override
            public Iterator<Selector> iterator()
            {
                return Selector.selectAll(graph, nodes.iterator(), contexts);
            }

        };
    }

    public static Iterator<Selector> selectAll(final Model graph,
            final Iterator<? extends Resource> nodes, final Resource... contexts)
    {
        Preconditions.checkNotNull(graph);
        Preconditions.checkNotNull(nodes);
        Preconditions.checkNotNull(contexts);

        return new UnmodifiableIterator<Selector>() {

            @Override
            public boolean hasNext()
            {
                return nodes.hasNext();
            }

            @Override
            public Selector next()
            {
                return new Selector(graph, nodes.next(), contexts);
            }

        };
    }

    public Iterable<Selector> selectAll(final IRI predicate)
    {
        return Selector.selectAll(this.graph, this.getAll(predicate, Resource.class),
                this.contexts);
    }

    // ATTRIBUTES

    public Model graph()
    {
        return this.graph;
    }

    public Selector graph(final Model graph)
    {
        Preconditions.checkNotNull(graph);

        return new Selector(graph, this.node, this.contexts);
    }

    public Resource node()
    {
        return this.node;
    }

    public Selector node(final Resource node)
    {
        Preconditions.checkNotNull(node);

        return new Selector(this.graph, node, this.contexts);
    }

    public Resource[] contexts()
    {
        return this.contexts;
    }

    public Selector contexts(final Resource... context)
    {
        Preconditions.checkNotNull(this.contexts);

        return new Selector(this.graph, this.node, this.contexts);
    }

    public Set<IRI> properties()
    {
        final Set<IRI> properties = Sets.newHashSet();
        final Iterator<Statement> i = this.match(this.node, null, null);
        while (i.hasNext()) {
            properties.add(i.next().getPredicate());
        }
        return properties;
    }

    // RETRIEVE

    public boolean isEmpty(final IRI predicate)
    {
        Preconditions.checkNotNull(predicate);

        return !this.match(this.node, predicate, null).hasNext();
    }

    public boolean isUnique(final IRI predicate)
    {
        Preconditions.checkNotNull(predicate);

        final Iterator<Statement> i = this.match(this.node, predicate, null);
        if (!i.hasNext()) {
            return false;
        }
        i.next();
        return !i.hasNext();
    }

    public boolean isSet(final IRI predicate)
    {
        Preconditions.checkNotNull(predicate);

        final Iterator<Statement> i = this.match(this.node, predicate, null);
        return i.hasNext() && ((Literal) Iterators.getOnlyElement(i).getObject()).booleanValue();
    }

    public boolean isInstanceOf(final Resource classResource)
    {
        Preconditions.checkNotNull(classResource);

        return this.match(this.node, RDF.TYPE, classResource).hasNext();
    }

    public <T> T get(final IRI predicate, final Class<T> type)
    {
        Preconditions.checkNotNull(predicate);

        final Iterator<Statement> i = this.match(this.node, predicate, null);
        return this.valueTo(Iterators.getOnlyElement(i).getObject(), type);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(final IRI predicate, final T defaultValue)
    {
        Preconditions.checkNotNull(predicate);
        Preconditions.checkNotNull(defaultValue);

        final Iterator<Statement> i = this.match(this.node, predicate, null);
        return i.hasNext() ? this.valueTo(Iterators.getOnlyElement(i).getObject(),
                (Class<T>) defaultValue.getClass()) : defaultValue;
    }

    public <T> T get(final IRI predicate, final Class<T> type, @Nullable final T defaultValue)
    {
        Preconditions.checkNotNull(predicate);
        Preconditions.checkNotNull(type);

        final Iterator<Statement> i = this.match(this.node, predicate, null);
        return i.hasNext() ? this.valueTo(Iterators.getOnlyElement(i).getObject(), type)
                : defaultValue;
    }

    public <T> List<T> getAll(final IRI predicate, final Class<T> type)
    {
        Preconditions.checkNotNull(predicate);
        Preconditions.checkNotNull(type);

        final List<T> result = Lists.newArrayList();
        final Iterator<Statement> i = this.match(this.node, predicate, null);
        while (i.hasNext()) {
            result.add(this.valueTo(i.next().getObject(), type));
        }
        return result;
    }

    public <T> List<T> getList(final IRI predicate, final Class<T> type)
    {
        Preconditions.checkNotNull(type);

        final List<T> result = Lists.newArrayList();
        Resource node = this.get(predicate, Resource.class, null);
        while (node != null && !RDF.NIL.equals(node)) {
            Iterator<Statement> i = this.match(node, RDF.FIRST, null);
            result.add(this.valueTo(Iterators.getOnlyElement(i).getObject(), type));
            i = this.match(node, RDF.REST, null);
            node = i.hasNext() ? (Resource) i.next().getObject() : null;
        }
        return result;
    }

    // EDITING

    public Selector set(final IRI predicate, final Object... objects)
    {
        Preconditions.checkNotNull(objects);

        this.remove(predicate);
        this.add(predicate, objects);
        return this;
    }

    public Selector set(final IRI predicate, final Iterable<?> objects)
    {
        Preconditions.checkNotNull(objects);

        this.remove(predicate);
        this.add(predicate, objects);
        return this;
    }

    public Selector add(final IRI predicate, final Object... objects)
    {
        Preconditions.checkNotNull(predicate);

        for (final Object object : objects) {
            this.graph.add(this.node, predicate, this.valueFrom(object), this.contexts);
        }
        return this;
    }

    public Selector add(final IRI predicate, final Iterable<?> objects)
    {
        Preconditions.checkNotNull(predicate);

        for (final Object object : objects) {
            this.graph.add(this.node, predicate, this.valueFrom(object), this.contexts);
        }
        return this;
    }

    public Selector remove(final IRI predicate)
    {
        Preconditions.checkNotNull(predicate);

        Iterators.removeIf(this.match(this.node, predicate, null), Predicates.alwaysTrue());
        return this;
    }

    public Selector remove(final IRI predicate, final Object... objects)
    {
        Preconditions.checkNotNull(predicate);

        for (final Object object : objects) {
            Iterators.removeIf(this.match(this.node, predicate, this.valueFrom(object)),
                    Predicates.alwaysTrue());
        }
        return this;
    }

    public Selector remove(final IRI predicate, final Iterable<?> objects)
    {
        Preconditions.checkNotNull(predicate);

        for (final Object object : objects) {
            Iterators.removeIf(this.match(this.node, predicate, this.valueFrom(object)),
                    Predicates.alwaysTrue());
        }
        return this;
    }

    // EQUALITY AND TO STRING

    @Override
    public boolean equals(final Object object)
    {
        if (object == this) {
            return true;
        }
        if (object == null || !(object instanceof Selector)) {
            return false;
        }
        final Selector other = (Selector) object;
        return this.graph == other.graph && this.node.equals(other.node)
                && Arrays.deepEquals(this.contexts, other.contexts);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(System.identityHashCode(this.graph), this.node,
                Arrays.hashCode(this.contexts));
    }

    @Override
    public String toString()
    {
        return this.node.stringValue();
    }

    // UTILITIES

    private Value valueFrom(final Object object)
    {
        Preconditions.checkNotNull(object);

        if (object instanceof Value) {
            return (Value) object;
        }

        final ValueFactory factory = SimpleValueFactory.getInstance();

        if (object instanceof Character) {
            return factory.createLiteral("" + object, XMLSchema.TOKEN);

        } else if (object instanceof Number) {
            if (object instanceof BigInteger) {
                return factory.createLiteral(object.toString(), XMLSchema.INTEGER);
            } else if (object instanceof Long) {
                return factory.createLiteral(((Long) object).longValue());
            } else if (object instanceof Integer) {
                return factory.createLiteral(((Integer) object).intValue());
            } else if (object instanceof Short) {
                return factory.createLiteral(((Short) object).shortValue());
            } else if (object instanceof Byte) {
                return factory.createLiteral(((Byte) object).byteValue());
            } else if (object instanceof BigDecimal) {
                return factory.createLiteral(object.toString(), XMLSchema.DECIMAL);
            } else if (object instanceof Double) {
                return factory.createLiteral(((Double) object).doubleValue());
            } else if (object instanceof Float) {
                return factory.createLiteral(((Float) object).floatValue());
            } else {
                return factory.createLiteral(object.toString());
            }

        } else if (object instanceof XMLGregorianCalendar) {
            return factory.createLiteral((XMLGregorianCalendar) object);
        } else if (object instanceof GregorianCalendar) {
            return factory.createLiteral(
                    Selector.DATATYPE_FACTORY.newXMLGregorianCalendar((GregorianCalendar) object));
        } else if (object instanceof Calendar) {
            final GregorianCalendar calendar = new GregorianCalendar();
            calendar.setTime(((Calendar) object).getTime());
            return factory
                    .createLiteral(Selector.DATATYPE_FACTORY.newXMLGregorianCalendar(calendar));
        } else if (object instanceof Date) {
            final GregorianCalendar calendar = new GregorianCalendar();
            calendar.setTime((Date) object);
            return factory
                    .createLiteral(Selector.DATATYPE_FACTORY.newXMLGregorianCalendar(calendar));

        } else {
            final String string = object.toString();
            if (string.startsWith("http://")) {
                try {
                    return factory.createIRI(string);
                } catch (final Throwable ex) {
                    // Ignore.
                }
            }
            return factory.createLiteral(string);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T valueTo(final Value value, final Class<T> type)
    {
        Preconditions.checkNotNull(value);
        Preconditions.checkNotNull(type);

        if (Value.class.isAssignableFrom(type)) {
            return (T) value;
        }

        final Literal literal = (Literal) value;

        if (type == String.class) {
            return (T) literal.stringValue();
        } else if (type == Boolean.class || type == boolean.class) {
            return (T) (Boolean) literal.booleanValue();
        } else if (type == Character.class || type == char.class) {
            return (T) (Character) literal.stringValue().charAt(0);
        } else if (type == Byte.class || type == byte.class) {
            return (T) (Byte) literal.byteValue();
        } else if (type == Short.class || type == short.class) {
            return (T) (Short) literal.shortValue();
        } else if (type == Integer.class || type == int.class) {
            return (T) (Integer) literal.intValue();
        } else if (type == Long.class || type == long.class) {
            return (T) (Long) literal.longValue();
        } else if (type == Float.class || type == float.class) {
            return (T) (Float) literal.floatValue();
        } else if (type == Double.class || type == double.class) {
            return (T) (Double) literal.doubleValue();
        } else if (type == Calendar.class) {
            return (T) literal.calendarValue().toGregorianCalendar();
        } else if (type == Date.class) {
            return (T) literal.calendarValue().toGregorianCalendar().getTime();
        } else {
            throw new IllegalArgumentException("Invalid type: " + type.getName());
        }
    }

    private Iterator<Statement> match(final Resource subject, final IRI predicate,
            final Value object)
    {
        return this.graph.filter(subject, predicate, object, this.contexts).iterator();
    }

}
