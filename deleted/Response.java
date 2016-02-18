package eu.fbk.dkm.internal.springles.protocol;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.openrdf.model.Literal;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.ListBindingSet;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.repository.RepositoryException;

import info.aduna.iteration.CloseableIteration;

import eu.fbk.dkm.internal.util.Iterations;
import eu.fbk.dkm.springles.ClosureStatus;

public final class Response<T> implements Serializable
{

    private static final long serialVersionUID = -8728557976394060268L;

    /**
	 * @uml.property  name="type"
	 * @uml.associationEnd  multiplicity="(1 1)"
	 */
    private final ResponseType<T> type;

    /**
	 * @uml.property  name="content"
	 */
    private final T content;

    public static <T> Response<T> create(final ResponseType<T> contentType, final T content)
    {
        return new Response<T>(contentType, content);
    }

    private Response(final ResponseType<T> contentType, final T content)
    {
        Preconditions.checkNotNull(contentType);
        Preconditions.checkNotNull(content);

        this.type = contentType;
        this.content = content;
    }

    public ResponseType<T> getType()
    {
        return this.type;
    }

    public T getContent()
    {
        return this.content;
    }

    @Override
    public boolean equals(final Object object)
    {
        if (object == this) {
            return true;
        }
        if (object == null || !(object instanceof Response<?>)) {
            return false;
        }
        final Response<?> other = (Response<?>) object;
        return other.type == this.type && other.content.equals(this.content);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(this.type.name, this.content);
    }

    @Override
    public String toString()
    {
        return this.type.toString().toLowerCase() + " response";
    }

    @SuppressWarnings("unchecked")
    public TupleQueryResult toTupleResult()
    {
        if (this.type == ResponseType.SETTINGS) {
            final BindingSet bindings = ((Settings) this.content).toBindings();
            return new TupleQueryResultImpl(ImmutableList.<String>copyOf(bindings
                    .getBindingNames()), ImmutableList.of(bindings));

        } else if (this.type == ResponseType.NAMESPACES) {
            return Iterations.asTupleQueryResult((CloseableIteration<? extends Namespace, //
                    RepositoryException>) this.content, "prefix", "name");

        } else if (this.type == ResponseType.TUPLE_RESULT) {
            return (TupleQueryResult) this.content;

        } else if (this.type == ResponseType.GRAPH_RESULT) {
            return Iterations.asTupleQueryResult((GraphQueryResult) this.content);

        } else if (this.type == ResponseType.RESOURCES) {
            return Iterations.asTupleQueryResult((CloseableIteration<? extends Resource, //
                    RepositoryException>) this.content, "context");

        } else if (this.type == ResponseType.STATEMENTS) {
            return Iterations.asTupleQueryResult((CloseableIteration<? extends Statement, //
                    RepositoryException>) this.content);

        } else if (this.type == ResponseType.CLOSURE_STATUS) {
            final List<String> names = ImmutableList.of("result");
            return new TupleQueryResultImpl(names, ImmutableList.of(new ListBindingSet(names,
                    new LiteralImpl(this.content.toString(), XMLSchema.STRING))));

        } else if (this.type == ResponseType.BOOLEAN) {
            final List<String> names = ImmutableList.of("result");
            return new TupleQueryResultImpl(names, ImmutableList.of(new ListBindingSet(names,
                    new LiteralImpl(this.content.toString(), XMLSchema.BOOLEAN))));

        } else if (this.type == ResponseType.LONG) {
            final List<String> names = ImmutableList.of("result");
            return new TupleQueryResultImpl(names, ImmutableList.of(new ListBindingSet(names,
                    new LiteralImpl(this.content.toString(), XMLSchema.LONG))));

        } else {
            throw new Error("Unexpected content type: " + this.content);
        }
    }

    public static <T> Response<T> fromTupleResult(final TupleQueryResult result,
            final ResponseType<T> contentType) throws QueryEvaluationException,
            RepositoryException
    {
        Preconditions.checkNotNull(result);
        Preconditions.checkNotNull(contentType);

        final Object content;
        if (contentType == ResponseType.SETTINGS) {
            content = Settings.fromBindings(Iterations.getOnlyElement(result));

        } else if (contentType == ResponseType.NAMESPACES) {
            content = Iterations.asNamespaceIteration(result, "parameter", "value");

        } else if (contentType == ResponseType.TUPLE_RESULT) {
            content = result;

        } else if (contentType == ResponseType.GRAPH_RESULT) {
            content = Iterations.asGraphQueryResult(result,
                    Collections.<String, String>emptyMap(), ValueFactoryImpl.getInstance());

        } else if (contentType == ResponseType.RESOURCES) {
            content = Iterations.project(result, "context", Resource.class);

        } else if (contentType == ResponseType.STATEMENTS) {
            content = Iterations.asGraphQueryResult(result,
                    Collections.<String, String>emptyMap(), ValueFactoryImpl.getInstance());

        } else if (contentType == ResponseType.CLOSURE_STATUS) {
            content = ClosureStatus.valueOf(((Literal) Iterations.getOnlyElement(result).getValue(
                    "result")).stringValue());

        } else if (contentType == ResponseType.BOOLEAN) {
            content = Boolean.valueOf(((Literal) Iterations.getOnlyElement(result).getValue(
                    "result")).booleanValue());

        } else if (contentType == ResponseType.LONG) {
            content = Long
                    .valueOf(((Literal) Iterations.getOnlyElement(result).getValue("result"))
                            .longValue());

        } else {
            throw new Error("Unexpected result type: " + result);
        }

        @SuppressWarnings("unchecked")
        final T castedContent = (T) content;
        return new Response<T>(contentType, castedContent);
    }

    /**
	 * @author  calabrese
	 */
    public static final class ResponseType<T> implements Serializable
    {

        private static final long serialVersionUID = -83245153431308660L;

        /**
		 * @uml.property  name="sETTINGS"
		 * @uml.associationEnd  
		 */
        public static final ResponseType<Settings> SETTINGS = create("SETTINGS");

        /**
		 * @uml.property  name="nAMESPACES"
		 * @uml.associationEnd  
		 */
        public static final ResponseType<CloseableIteration<? extends Namespace, //
        RepositoryException>> NAMESPACES = create("NAMESPACES");

        /**
		 * @uml.property  name="tUPLE_RESULT"
		 * @uml.associationEnd  
		 */
        public static final ResponseType<TupleQueryResult> TUPLE_RESULT = create("TUPLE_RESULT");

        /**
		 * @uml.property  name="gRAPH_RESULT"
		 * @uml.associationEnd  
		 */
        public static final ResponseType<GraphQueryResult> GRAPH_RESULT = create("GRAPH_RESULT");

        /**
		 * @uml.property  name="rESOURCES"
		 * @uml.associationEnd  
		 */
        public static final ResponseType<CloseableIteration<? extends Resource, //
        RepositoryException>> RESOURCES = create("RESOURCES");

        /**
		 * @uml.property  name="sTATEMENTS"
		 * @uml.associationEnd  
		 */
        public static final ResponseType<CloseableIteration<? extends Statement, //
        RepositoryException>> STATEMENTS = create("STATEMENTS");

        /**
		 * @uml.property  name="cLOSURE_STATUS"
		 * @uml.associationEnd  
		 */
        public static final ResponseType<ClosureStatus> CLOSURE_STATUS = create("CLOSURE_STATUS");

        /**
		 * @uml.property  name="bOOLEAN"
		 * @uml.associationEnd  
		 */
        public static final ResponseType<Boolean> BOOLEAN = create("BOOLEAN");

        /**
		 * @uml.property  name="lONG"
		 * @uml.associationEnd  
		 */
        public static final ResponseType<Long> LONG = create("LONG");

        private static final Map<String, ResponseType<?>> MAP = ImmutableMap
                .<String, ResponseType<?>>builder().put(NAMESPACES.name, NAMESPACES) //
                .put(TUPLE_RESULT.name, TUPLE_RESULT) //
                .put(GRAPH_RESULT.name, GRAPH_RESULT) //
                .put(RESOURCES.name, RESOURCES) //
                .put(STATEMENTS.name, STATEMENTS) //
                .put(CLOSURE_STATUS.name, CLOSURE_STATUS) //
                .put(BOOLEAN.name, BOOLEAN) //
                .put(LONG.name, LONG).build();

        private final String name;

        private static <T> ResponseType<T> create(final String name)
        {
            return new ResponseType<T>(name);
        }

        private ResponseType(final String name)
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return this.name;
        }

        public static Iterable<ResponseType<?>> values()
        {
            return MAP.values();
        }

        public static ResponseType<?> valueOf(final String string)
        {
            final ResponseType<?> result = MAP.get(string);
            Preconditions.checkArgument(result != null, "Invalid response type: " + string);
            return result;
        }

        public static <T extends Query> ResponseType<?> forQueryClass(final Class<T> queryClass)
        {
            Preconditions.checkNotNull(queryClass);
            if (queryClass == BooleanQuery.class) {
                return BOOLEAN;
            } else if (queryClass == TupleQuery.class) {
                return TUPLE_RESULT;
            } else if (queryClass == GraphQuery.class) {
                return GRAPH_RESULT;
            } else {
                throw new Error("Unknown query class: " + queryClass.getName());
            }
        }

        private Object readResolve() throws ObjectStreamException
        {
            return valueOf(this.name);
        }

    }

}
