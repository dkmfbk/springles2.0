package eu.fbk.dkm.springles.base;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;

public final class QueryType<T> implements Serializable
{

    private static final long serialVersionUID = 4618601975693917537L;

    public static final QueryType<Boolean> BOOLEAN = new QueryType<Boolean>(Boolean.class,
            BooleanQuery.class, ParsedBooleanQuery.class, "BOOLEAN");

    public static final QueryType<TupleQueryResult> TUPLE = new QueryType<TupleQueryResult>(
            TupleQueryResult.class, TupleQuery.class, ParsedTupleQuery.class, "TUPLE");

    public static final QueryType<GraphQueryResult> GRAPH = new QueryType<GraphQueryResult>(
            GraphQueryResult.class, GraphQuery.class, ParsedGraphQuery.class, "GRAPH");

    private static final List<QueryType<?>> VALUES = ImmutableList.of(QueryType.BOOLEAN,
            QueryType.TUPLE, QueryType.GRAPH);

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
        return QueryType.VALUES;
    }

    public static QueryType<?> valueOf(final String string)
    {
        if (QueryType.BOOLEAN.name.equals(string)) {
            return QueryType.BOOLEAN;
        } else if (QueryType.TUPLE.name.equals(string)) {
            return QueryType.TUPLE;
        } else if (QueryType.GRAPH.name.equals(string)) {
            return QueryType.GRAPH;
        } else {
            throw new IllegalArgumentException("Unknown query type: " + string);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> QueryType<T> forResultClass(final Class<T> resultClass)
    {
        Preconditions.checkNotNull(resultClass);
        if (QueryType.BOOLEAN.resultClass.isAssignableFrom(resultClass)) {
            return (QueryType<T>) QueryType.BOOLEAN;
        } else if (QueryType.TUPLE.resultClass.isAssignableFrom(resultClass)) {
            return (QueryType<T>) QueryType.TUPLE;
        } else if (QueryType.GRAPH.resultClass.isAssignableFrom(resultClass)) {
            return (QueryType<T>) QueryType.GRAPH;
        } else {
            throw new IllegalArgumentException(
                    "Invalid query result class: " + resultClass.getName());
        }
    }

    public static QueryType<?> forQueryClass(final Class<? extends Query> queryClass)
    {
        Preconditions.checkNotNull(queryClass);
        if (QueryType.BOOLEAN.queryClass.isAssignableFrom(queryClass)) {
            return QueryType.BOOLEAN;
        } else if (QueryType.TUPLE.queryClass.isAssignableFrom(queryClass)) {
            return QueryType.TUPLE;
        } else if (QueryType.GRAPH.queryClass.isAssignableFrom(queryClass)) {
            return QueryType.GRAPH;
        } else {
            throw new IllegalArgumentException("Invalid query class: " + queryClass.getName());
        }
    }

    public static QueryType<?> forParsedClass(final Class<? extends ParsedQuery> parsedClass)
    {
        Preconditions.checkNotNull(parsedClass);
        if (QueryType.BOOLEAN.parsedClass.isAssignableFrom(parsedClass)) {
            return QueryType.BOOLEAN;
        } else if (QueryType.TUPLE.parsedClass.isAssignableFrom(parsedClass)) {
            return QueryType.TUPLE;
        } else if (QueryType.GRAPH.parsedClass.isAssignableFrom(parsedClass)) {
            return QueryType.GRAPH;
        } else {
            throw new IllegalArgumentException("Invalid query class: " + parsedClass.getName());
        }
    }

    private Object readResolve() throws ObjectStreamException
    {
        return QueryType.forResultClass(this.resultClass);
    }

}
