package eu.fbk.dkm.springles.base;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.Query;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.parser.ParsedBooleanQuery;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.ParsedTupleQuery;

public final class QueryType<T> implements Serializable
{

    private static final long serialVersionUID = 4618601975693917537L;

    public static final QueryType<Boolean> BOOLEAN = new QueryType<Boolean>(Boolean.class,
            BooleanQuery.class, ParsedBooleanQuery.class, "BOOLEAN");

    public static final QueryType<TupleQueryResult> TUPLE = new QueryType<TupleQueryResult>(
            TupleQueryResult.class, TupleQuery.class, ParsedTupleQuery.class, "TUPLE");

    public static final QueryType<GraphQueryResult> GRAPH = new QueryType<GraphQueryResult>(
            GraphQueryResult.class, GraphQuery.class, ParsedGraphQuery.class, "GRAPH");

    private static final List<QueryType<?>> VALUES = ImmutableList.of(BOOLEAN, TUPLE, GRAPH);

    private final Class<T> resultClass;

    private final Class<? extends Query> queryClass;

    private final Class<? extends ParsedQuery> parsedClass;

    private final String name;

    private QueryType(final Class<T> resultClass, final Class<? extends Query> queryClass,
            final Class<? extends ParsedQuery> parsedClass, final String name)
    {
        this.resultClass = resultClass;
        this.queryClass = queryClass;
        this.parsedClass = parsedClass;
        this.name = name;
    }

    public Class<T> getResultClass()
    {
        return this.resultClass;
    }

    public Class<? extends Query> getQueryClass()
    {
        return this.queryClass;
    }

    public Class<? extends ParsedQuery> getParsedClass()
    {
        return this.parsedClass;
    }

    @Override
    public String toString()
    {
        return this.name;
    }

    public static Iterable<QueryType<?>> values()
    {
        return VALUES;
    }

    public static QueryType<?> valueOf(final String string)
    {
        if (BOOLEAN.name.equals(string)) {
            return BOOLEAN;
        } else if (TUPLE.name.equals(string)) {
            return TUPLE;
        } else if (GRAPH.name.equals(string)) {
            return GRAPH;
        } else {
            throw new IllegalArgumentException("Unknown query type: " + string);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> QueryType<T> forResultClass(final Class<T> resultClass)
    {
        Preconditions.checkNotNull(resultClass);
        if (BOOLEAN.resultClass.isAssignableFrom(resultClass)) {
            return (QueryType<T>) BOOLEAN;
        } else if (TUPLE.resultClass.isAssignableFrom(resultClass)) {
            return (QueryType<T>) TUPLE;
        } else if (GRAPH.resultClass.isAssignableFrom(resultClass)) {
            return (QueryType<T>) GRAPH;
        } else {
            throw new IllegalArgumentException("Invalid query result class: "
                    + resultClass.getName());
        }
    }

    public static QueryType<?> forQueryClass(final Class<? extends Query> queryClass)
    {
        Preconditions.checkNotNull(queryClass);
        if (BOOLEAN.queryClass.isAssignableFrom(queryClass)) {
            return BOOLEAN;
        } else if (TUPLE.queryClass.isAssignableFrom(queryClass)) {
            return TUPLE;
        } else if (GRAPH.queryClass.isAssignableFrom(queryClass)) {
            return GRAPH;
        } else {
            throw new IllegalArgumentException("Invalid query class: " + queryClass.getName());
        }
    }

    public static QueryType<?> forParsedClass(final Class<? extends ParsedQuery> parsedClass)
    {
        Preconditions.checkNotNull(parsedClass);
        if (BOOLEAN.parsedClass.isAssignableFrom(parsedClass)) {
            return BOOLEAN;
        } else if (TUPLE.parsedClass.isAssignableFrom(parsedClass)) {
            return TUPLE;
        } else if (GRAPH.parsedClass.isAssignableFrom(parsedClass)) {
            return GRAPH;
        } else {
            throw new IllegalArgumentException("Invalid query class: " + parsedClass.getName());
        }
    }

    private Object readResolve() throws ObjectStreamException
    {
        return forResultClass(this.resultClass);
    }

}
