package eu.fbk.dkm.internal.springles.protocol;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.query.impl.ListBindingSet;
import org.openrdf.rio.RDFFormat;

import eu.fbk.dkm.internal.springles.protocol.Response.ResponseType;
import eu.fbk.dkm.internal.util.RDFParseOptions;
import eu.fbk.dkm.internal.util.RDFSource;
import eu.fbk.dkm.springles.InferenceMode;

public final class Request implements Serializable
{

    private static final long serialVersionUID = 1683112979551390736L;

    private static final Map<String, Class<?>> TYPE_MAP;

    /**
	 * @uml.property  name="command"
	 * @uml.associationEnd  multiplicity="(1 1)"
	 */
    private final Command command;

    /**
	 * @uml.property  name="args"
	 * @uml.associationEnd  multiplicity="(0 -1)" ordering="true" elementType="java.lang.Object" qualifier="argument:eu.fbk.dkm.internal.springles.protocol.Request$Argument java.lang.Object"
	 */
    private final Map<Argument<?>, Object> args;

    public Request(final Command command)
    {
        Preconditions.checkNotNull(command);

        this.command = command;
        this.args = Maps.newHashMap();
    }

    /**
	 * @return
	 * @uml.property  name="command"
	 */
    public Command getCommand()
    {
        return this.command;
    }

    public Map<Argument<?>, Object> getArgs()
    {
        return this.args;
    }

    public <T> T getArg(final Argument<T> argument)
    {
        @SuppressWarnings("unchecked")
        T result = (T) this.args.get(argument);
        if (result == null && argument.isDefaultValueAvailable()) {
            result = argument.getDefaultValue();
        }
        return result;
    }

    public <T> Request setArg(final Argument<T> argument, final T value)
    {
        Preconditions.checkNotNull(argument);
        Preconditions.checkNotNull(value);
        this.args.put(argument, value);
        return this;
    }

    public void validate()
    {
        for (final Argument<?> argument : this.command.getArguments()) {
            if (!this.args.containsKey(argument) && !argument.isDefaultValueAvailable()) {
                throw new IllegalArgumentException("Missing mandatory argument " + argument
                        + " for command " + this.command);
            }
        }
    }

    @Override
    public boolean equals(final Object object)
    {
        if (object == this) {
            return true;
        }
        if (object == null || !(object instanceof Request)) {
            return false;
        }
        final Request other = (Request) object;
        return other.command == this.command && other.args.equals(this.args);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(this.command, this.args);
    }

    @Override
    public String toString()
    {
        return this.command.toString() + "("
                + Joiner.on(", ").withKeyValueSeparator(" = ").join(this.args) + ")";
    }

    public String toXML()
    {
        try {
            final StringWriter writer = new StringWriter();
            final XMLStreamWriter out = XMLOutputFactory.newInstance().createXMLStreamWriter(
                    writer);

            out.writeStartDocument("utf-8", "1.0");
            out.writeCharacters("\n");
            out.writeStartElement("request");
            out.writeAttribute("name", this.command.getName());

            for (final Map.Entry<Argument<?>, Object> entry : this.args.entrySet()) {

                final Argument<?> argument = entry.getKey();
                final Object value = entry.getValue();

                if (value != null) {
                    Preconditions.checkArgument(argument.getType().isInstance(value));
                    out.writeCharacters("\n  ");
                    out.writeStartElement("arg");
                    out.writeAttribute("name", argument.toString());
                    encodeObject(out, value, argument.getType(), "\n  ");
                    out.writeEndElement();
                }
            }

            out.writeCharacters("\n");
            out.writeEndElement();
            out.writeEndDocument();

            return writer.toString();

        } catch (final XMLStreamException ex) {
            throw new Error("Unexpected exception", ex);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Request fromXML(final String xml)
    {
        try {
            final StringReader reader = new StringReader(xml);
            final XMLStreamReader in = XMLInputFactory.newInstance().createXMLStreamReader(reader);

            in.nextTag();
            Preconditions.checkArgument("request".equals(in.getLocalName()), "<request> expected");
            final Command command = Command.fromName(in.getAttributeValue(null, "name"));

            final Request request = new Request(command);

            while (in.nextTag() == XMLStreamConstants.START_ELEMENT) {
                Preconditions.checkArgument("arg".equals(in.getLocalName()), "Expected <arg> tag");
                final Argument<?> argument = Argument.valueOf(in.getAttributeValue(null, "name"));
                if (!command.getArguments().contains(argument)) {
                    throw new IllegalArgumentException("Argument " + argument
                            + " unsupported for command " + command);
                }
                final Object value = decodeObject(in, argument.getType());
                request.setArg((Argument) argument, value);
            }

            return request;

        } catch (final XMLStreamException ex) {
            throw new Error("Unexpected exception", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private static void encodeObject(final XMLStreamWriter out, final Object value,
            final Class<?> baseType, final String indent) throws XMLStreamException
    {
        if (value instanceof BNode) {
            encodeType(out, BNode.class, baseType);
            out.writeCharacters(((BNode) value).getID());

        } else if (value instanceof Literal) {
            encodeType(out, Literal.class, baseType);
            final Literal literal = (Literal) value;
            if (literal.getDatatype() != null) {
                out.writeAttribute("datatype", literal.getDatatype().toString());
            } else if (literal.getLanguage() != null) {
                out.writeAttribute("language", literal.getLanguage());
            }
            out.writeCharacters(literal.getLabel());

        } else if (value instanceof Dataset) {
            encodeType(out, Dataset.class, baseType);
            final String newIndent = indent + "  ";
            final Dataset dataset = (Dataset) value;
            for (final URI uri : dataset.getDefaultGraphs()) {
                out.writeCharacters(newIndent);
                out.writeStartElement("default");
                out.writeCharacters(uri.toString());
                out.writeEndElement();
            }
            for (final URI uri : dataset.getNamedGraphs()) {
                out.writeCharacters(newIndent);
                out.writeStartElement("named");
                out.writeCharacters(uri.toString());
                out.writeEndElement();
            }
            for (final URI uri : dataset.getDefaultRemoveGraphs()) {
                out.writeCharacters(newIndent);
                out.writeStartElement("remove");
                out.writeCharacters(uri.toString());
                out.writeEndElement();
            }
            if (dataset.getDefaultInsertGraph() != null) {
                out.writeCharacters(newIndent);
                out.writeStartElement("insert");
                out.writeCharacters(dataset.getDefaultInsertGraph().toString());
                out.writeEndElement();
            }
            out.writeCharacters(indent);

        } else if (value instanceof BindingSet) {
            encodeType(out, BindingSet.class, baseType);
            final String newIndent = indent + "  ";
            final BindingSet bindings = (BindingSet) value;
            for (final Binding binding : bindings) {
                out.writeCharacters(newIndent);
                out.writeStartElement("binding");
                out.writeAttribute("name", binding.getName());
                encodeObject(out, binding.getValue(), Value.class, indent + "  ");
                out.writeEndElement();
            }
            out.writeCharacters(indent);

        } else if (value.getClass().isArray()) {
            encodeType(out, value.getClass(), baseType);
            final String newIndent = indent + "  ";
            final int length = Array.getLength(value);
            final Class<?> elementClass = value.getClass().getComponentType();
            for (int i = 0; i < length; ++i) {
                out.writeCharacters(newIndent);
                out.writeStartElement("item");
                encodeObject(out, Array.get(value, i), elementClass, indent + "  ");
                out.writeEndElement();
            }
            out.writeCharacters(indent);

        } else if (value instanceof Iterable<?>) {
            encodeType(out, List.class, baseType);
            final StringWriter writer = new StringWriter();
            try {
                RDFSource.wrap((Iterable<Statement>) value).serializeTo(RDFFormat.TRIG, writer);
            } catch (final IOException ex) {
                throw new Error("Unexpected exception", ex);
            }
            out.writeCharacters(writer.toString());

        } else {
            encodeType(out, value.getClass(), baseType);
            out.writeCharacters(value.toString());
        }
    }

    private static void encodeType(final XMLStreamWriter out, final Class<?> type,
            final Class<?> baseType) throws XMLStreamException
    {
        if (type != baseType) {
            for (final Map.Entry<String, Class<?>> entry : TYPE_MAP.entrySet()) {
                if (entry.getValue().isAssignableFrom(type)) {
                    if (entry.getValue() != String.class) {
                        out.writeAttribute("type", entry.getKey());
                    }
                    return;
                }
            }
            throw new Error("Unexpected type: " + type);
        }
    }

    @SuppressWarnings("unchecked")
    private static Object decodeObject(final XMLStreamReader in, final Class<?> baseType)
            throws XMLStreamException
    {
        final Class<?> type = decodeType(in, baseType);

        if (type == InferenceMode.class) {
            return InferenceMode.valueOf(decodeText(in));
        } else if (type == QueryLanguage.class) {
            return QueryLanguage.valueOf(decodeText(in));

        } else if (type == String.class) {
            return decodeText(in);
        } else if (type == Byte.class) {
            return Byte.valueOf(decodeText(in));
        } else if (type == Short.class) {
            return Short.valueOf(decodeText(in));
        } else if (type == Integer.class) {
            return Integer.valueOf(decodeText(in));
        } else if (type == Long.class) {
            return Long.valueOf(decodeText(in));
        } else if (type == Float.class) {
            return Float.valueOf(decodeText(in));
        } else if (type == Double.class) {
            return Double.valueOf(decodeText(in));
        } else if (type == URI.class) {
            return new URIImpl(decodeText(in));
        } else if (type == BNode.class) {
            return new BNodeImpl(decodeText(in));

        } else if (type == Literal.class) {
            final String datatype = in.getAttributeValue(null, "datatype");
            final String language = in.getAttributeValue(null, "language");
            final String label = decodeText(in);
            return datatype != null ? new LiteralImpl(label, new URIImpl(datatype))
                    : language != null ? new LiteralImpl(label, language) : new LiteralImpl(label);

        } else if (type == Dataset.class) {
            final DatasetImpl dataset = new DatasetImpl();
            while (in.nextTag() == XMLStreamConstants.START_ELEMENT) {
                if ("default".equals(in.getLocalName())) {
                    dataset.addDefaultGraph(new URIImpl(decodeText(in)));
                } else if ("named".equals(in.getLocalName())) {
                    dataset.addNamedGraph(new URIImpl(decodeText(in)));
                } else if ("remove".equals(in.getLocalName())) {
                    dataset.addDefaultRemoveGraph(new URIImpl(decodeText(in)));
                } else if ("insert".equals(in.getLocalName())) {
                    dataset.setDefaultInsertGraph(new URIImpl(decodeText(in)));
                } else {
                    throw new IllegalArgumentException(
                            "<default>, <named>, <remove> or <insert> expected");
                }
            }
            return dataset;

        } else if (type == BindingSet.class) {
            final List<String> names = Lists.newArrayList();
            final List<Value> values = Lists.newArrayList();
            while (in.nextTag() == XMLStreamConstants.START_ELEMENT) {
                Preconditions.checkArgument("binding".equals(in.getLocalName()),
                        "<binding> expected");
                names.add(in.getAttributeValue(null, "name"));
                values.add((Value) decodeObject(in, Value.class));
            }
            return new ListBindingSet(names, values);

        } else if (type.isArray()) {
            final List<Object> items = Lists.newArrayList();
            while (in.nextTag() == XMLStreamConstants.START_ELEMENT) {
                Preconditions.checkArgument("item".equals(in.getLocalName()), "<item> expected");
                items.add(decodeObject(in, type.getComponentType()));
            }
            return Iterables.toArray(items, (Class<Object>) type.getComponentType());

        } else if (type == Iterable.class) {
            try {
                return RDFSource.deserializeFrom(new StringReader(decodeText(in)),
                        new RDFParseOptions(RDFFormat.TRIG)).streamToGraph();
            } catch (final Throwable ex) {
                throw new IllegalArgumentException("Invalid RDF", ex);
            }

        } else {
            throw new Error("Unexpected type: " + type);
        }
    }

    private static String decodeText(final XMLStreamReader in) throws XMLStreamException
    {
        final StringBuilder builder = new StringBuilder();
        while (true) {
            final int event = in.next();
            if (event != XMLStreamConstants.CHARACTERS && event != XMLStreamConstants.SPACE) {
                break;
            }
            builder.append(in.getText());
        }
        return builder.toString();
    }

    private static Class<?> decodeType(final XMLStreamReader in, final Class<?> baseType)
            throws XMLStreamException
    {
        final String typeName = in.getAttributeValue(null, "type");
        if (typeName == null) {
            return baseType;
        } else {
            final Class<?> type = TYPE_MAP.get(typeName);
            Preconditions.checkArgument(type != null, "Unsupported value type " + typeName);
            Preconditions.checkArgument(baseType.isAssignableFrom(type), "Incompatible type "
                    + typeName + " (expected " + baseType.getName());
            return type;
        }
    }

    static {
        final Map<String, Class<?>> map = Maps.newHashMap();

        map.put("inferenceMode", InferenceMode.class);

        map.put("string", String.class);
        map.put("byte", Byte.class);
        map.put("short", Short.class);
        map.put("integer", Integer.class);
        map.put("long", Long.class);
        map.put("float", Float.class);
        map.put("double", Double.class);
        map.put("bnode", BNode.class);
        map.put("uri", URI.class);
        map.put("literal", Literal.class);

        map.put("objects", new Object[] {}.getClass());
        map.put("values", new Value[] {}.getClass());
        map.put("resources", new Resource[] {}.getClass());

        map.put("strings", new String[] {}.getClass());
        map.put("bytes", new Byte[] {}.getClass());
        map.put("shorts", new Short[] {}.getClass());
        map.put("integers", new Integer[] {}.getClass());
        map.put("longs", new Long[] {}.getClass());
        map.put("floats", new Float[] {}.getClass());
        map.put("doubles", new Double[] {}.getClass());
        map.put("bnodes", new BNode[] {}.getClass());
        map.put("uris", new URI[] {}.getClass());
        map.put("literals", new Literal[] {}.getClass());

        map.put("dataset", Dataset.class);
        map.put("bindings", BindingSet.class);
        map.put("rdf", Iterable.class); // assume list of statements

        TYPE_MAP = ImmutableMap.copyOf(map);
    }

    /**
	 * @author  calabrese
	 */
    public static final class Argument<T> implements Serializable
    {

        public static final long serialVersionUID = 4620268231449488118L;

        /**
		 * @uml.property  name="pREFIX"
		 * @uml.associationEnd  
		 */
        public static final Argument<String> PREFIX = create("prefix", String.class);

        /**
		 * @uml.property  name="nAME"
		 * @uml.associationEnd  
		 */
        public static final Argument<String> NAME = create("name", String.class, null);

        /**
		 * @uml.property  name="sUBJECT"
		 * @uml.associationEnd  
		 */
        public static final Argument<Resource> SUBJECT = create("subject", Resource.class, null);

        /**
		 * @uml.property  name="pREDICATE"
		 * @uml.associationEnd  
		 */
        public static final Argument<URI> PREDICATE = create("predicate", URI.class, null);

        /**
		 * @uml.property  name="oBJECT"
		 * @uml.associationEnd  
		 */
        public static final Argument<Value> OBJECT = create("object", Value.class, null);

        /**
		 * @uml.property  name="iNFERENCE_MODE"
		 * @uml.associationEnd  
		 */
        public static final Argument<InferenceMode> INFERENCE_MODE = create("inferenceMode",
                InferenceMode.class, InferenceMode.COMBINED);

        /**
		 * @uml.property  name="cONTEXTS"
		 * @uml.associationEnd  
		 */
        public static final Argument<Resource[]> CONTEXTS = create("contexts", Resource[].class,
                new Resource[] {});

        /**
		 * @uml.property  name="uPDATE_URI"
		 * @uml.associationEnd  
		 */
        public static final Argument<URI> UPDATE_URI = create("updateURI", URI.class);

        /**
		 * @uml.property  name="uPDATE_STRING"
		 * @uml.associationEnd  
		 */
        public static final Argument<String> UPDATE_STRING = create("updateString", String.class);

        /**
		 * @uml.property  name="qUERY_TYPE"
		 * @uml.associationEnd  
		 */
        public static final Argument<String> QUERY_TYPE = create("queryType", String.class);

        /**
		 * @uml.property  name="qUERY_STRING"
		 * @uml.associationEnd  
		 */
        public static final Argument<String> QUERY_STRING = create("queryString", String.class);

        /**
		 * @uml.property  name="qUERY_URI"
		 * @uml.associationEnd  
		 */
        public static final Argument<URI> QUERY_URI = create("queryURI", URI.class);

        /**
		 * @uml.property  name="lANGUAGE"
		 * @uml.associationEnd  
		 */
        public static final Argument<QueryLanguage> LANGUAGE = create("language",
                QueryLanguage.class, QueryLanguage.SPARQL);

        /**
		 * @uml.property  name="bASE_URI"
		 * @uml.associationEnd  
		 */
        public static final Argument<String> BASE_URI = create("baseURI", String.class, null);

        /**
		 * @uml.property  name="dATASET"
		 * @uml.associationEnd  
		 */
        public static final Argument<Dataset> DATASET = create("dataset", Dataset.class, null);

        /**
		 * @uml.property  name="bINDINGS"
		 * @uml.associationEnd  
		 */
        public static final Argument<BindingSet> BINDINGS = create("bindings", BindingSet.class,
                null);

        /**
		 * @uml.property  name="tIMEOUT"
		 * @uml.associationEnd  
		 */
        public static final Argument<Integer> TIMEOUT = create("timeout", Integer.class, 0);

        /**
		 * @uml.property  name="pARAMETERS"
		 * @uml.associationEnd  
		 */
        public static final Argument<Object[]> PARAMETERS = create("parameters", Object[].class,
                new Object[] {});

        /**
		 * @uml.property  name="sTATEMENTS"
		 * @uml.associationEnd  
		 */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public static final Argument<Iterable<? extends Statement>> STATEMENTS = (Argument) create(
                "statements", Iterable.class, null);

        /**
		 * @uml.property  name="cOMMIT"
		 * @uml.associationEnd  
		 */
        public static final Argument<Boolean> COMMIT = create("commit", Boolean.class,
                Boolean.TRUE);

        private static final Map<String, Argument<?>> MAP = ImmutableMap
                .<String, Argument<?>>builder().put(PREFIX.name, PREFIX).put(NAME.name, NAME)
                .put(SUBJECT.name, SUBJECT).put(PREDICATE.name, PREDICATE)
                .put(OBJECT.name, OBJECT).put(CONTEXTS.name, CONTEXTS)
                .put(UPDATE_URI.name, UPDATE_URI).put(UPDATE_STRING.name, UPDATE_STRING)
                .put(QUERY_TYPE.name, QUERY_TYPE).put(QUERY_STRING.name, QUERY_STRING)
                .put(QUERY_URI.name, QUERY_URI).put(LANGUAGE.name, LANGUAGE)
                .put(BASE_URI.name, BASE_URI).put(DATASET.name, DATASET)
                .put(BINDINGS.name, BINDINGS).put(TIMEOUT.name, TIMEOUT)
                .put(PARAMETERS.name, PARAMETERS).put(STATEMENTS.name, STATEMENTS)
                .put(COMMIT.name, COMMIT).build();

        private final String name;

        /**
		 * @uml.property  name="type"
		 */
        private final transient Class<T> type;

        /**
		 * @uml.property  name="defaultValueAvailable"
		 */
        private final transient boolean defaultValueAvailable;

        /**
		 * @uml.property  name="defaultValue"
		 */
        private final transient T defaultValue;

        private static <T> Argument<T> create(final String name, final Class<T> type)
        {
            return new Argument<T>(name, type, false, null);
        }

        private static <T> Argument<T> create(final String name, final Class<T> type,
                @Nullable final T defaultValue)
        {
            return new Argument<T>(name, type, true, defaultValue);
        }

        private Argument(final String name, final Class<T> type,
                final boolean defaultValueAvailable, @Nullable final T defaultValue)
        {
            this.name = name;
            this.type = type;
            this.defaultValueAvailable = defaultValueAvailable;
            this.defaultValue = defaultValue;
        }

        /**
		 * @return
		 * @uml.property  name="type"
		 */
        public Class<T> getType()
        {
            return this.type;
        }

        /**
		 * @return
		 * @uml.property  name="defaultValueAvailable"
		 */
        public boolean isDefaultValueAvailable()
        {
            return this.defaultValueAvailable;
        }

        /**
		 * @return
		 * @uml.property  name="defaultValue"
		 */
        public T getDefaultValue()
        {
            return this.defaultValue;
        }

        @Override
        public String toString()
        {
            return this.name;
        }

        public static Iterable<Argument<?>> values()
        {
            return MAP.values();
        }

        public static Argument<?> valueOf(final String string)
        {
            final Argument<?> argument = MAP.get(string);
            Preconditions.checkArgument(argument != null, "Invalid argument: " + string);
            return argument;
        }

        private Object readResolve() throws ObjectStreamException
        {
            return valueOf(this.name);
        }

    }

    /**
	 * @author   calabrese
	 */
    public enum Command
    {

        /**
		 * @uml.property  name="cONNECT"
		 * @uml.associationEnd  
		 */
        CONNECT("connect", ImmutableSet.of(ResponseType.SETTINGS)),

        /**
		 * @uml.property  name="gET_NAMESPACES"
		 * @uml.associationEnd  
		 */
        GET_NAMESPACES("getNamespaces", ImmutableSet.of(ResponseType.NAMESPACES)),

        /**
		 * @uml.property  name="sET_NAMESPACE"
		 * @uml.associationEnd  
		 */
        SET_NAMESPACE("setNamespace", Argument.PREFIX, Argument.NAME),

        /**
		 * @uml.property  name="cLEAR_NAMESPACES"
		 * @uml.associationEnd  
		 */
        CLEAR_NAMESPACES("clearNamespaces"),

        /**
		 * @uml.property  name="qUERY"
		 * @uml.associationEnd  
		 */
        QUERY("query", ImmutableSet.of(ResponseType.BOOLEAN, ResponseType.TUPLE_RESULT,
                ResponseType.GRAPH_RESULT), Argument.QUERY_TYPE, Argument.QUERY_STRING,
                Argument.LANGUAGE, Argument.BASE_URI, Argument.DATASET, Argument.BINDINGS,
                Argument.INFERENCE_MODE, Argument.TIMEOUT),

        /**
		 * @uml.property  name="qUERY_NAMED"
		 * @uml.associationEnd  
		 */
        QUERY_NAMED("queryNamed", ImmutableSet.of(ResponseType.BOOLEAN, ResponseType.TUPLE_RESULT,
                ResponseType.GRAPH_RESULT), Argument.QUERY_TYPE, Argument.QUERY_URI,
                Argument.INFERENCE_MODE, Argument.PARAMETERS),

        /**
		 * @uml.property  name="gET_CONTEXT_IDS"
		 * @uml.associationEnd  
		 */
        GET_CONTEXT_IDS("getContextIDs", ImmutableSet.of(ResponseType.RESOURCES),
                Argument.INFERENCE_MODE),

        /**
		 * @uml.property  name="gET_STATEMENTS"
		 * @uml.associationEnd  
		 */
        GET_STATEMENTS("getStatements", ImmutableSet.of(ResponseType.STATEMENTS),
                Argument.SUBJECT, Argument.PREDICATE, Argument.OBJECT, Argument.INFERENCE_MODE,
                Argument.CONTEXTS),

        /**
		 * @uml.property  name="hAS_STATEMENT"
		 * @uml.associationEnd  
		 */
        HAS_STATEMENT("hasStatements", ImmutableSet.of(ResponseType.BOOLEAN), Argument.SUBJECT,
                Argument.PREDICATE, Argument.OBJECT, Argument.INFERENCE_MODE, Argument.CONTEXTS),

        /**
		 * @uml.property  name="sIZE"
		 * @uml.associationEnd  
		 */
        SIZE("size", ImmutableSet.of(ResponseType.LONG), Argument.INFERENCE_MODE,
                Argument.CONTEXTS),

        /**
		 * @uml.property  name="uPDATE"
		 * @uml.associationEnd  
		 */
        UPDATE("update", Argument.UPDATE_STRING, Argument.LANGUAGE, Argument.BASE_URI,
                Argument.DATASET, Argument.BINDINGS, Argument.INFERENCE_MODE),

        /**
		 * @uml.property  name="uPDATE_NAMED"
		 * @uml.associationEnd  
		 */
        UPDATE_NAMED("updateNamed", Argument.UPDATE_URI, Argument.INFERENCE_MODE,
                Argument.PARAMETERS),

        /**
		 * @uml.property  name="aDD"
		 * @uml.associationEnd  
		 */
        ADD("add", Argument.STATEMENTS, Argument.CONTEXTS),

        /**
		 * @uml.property  name="rEMOVE"
		 * @uml.associationEnd  
		 */
        REMOVE("remove", Argument.STATEMENTS, Argument.CONTEXTS),

        /**
		 * @uml.property  name="rEMOVE_MATCHING"
		 * @uml.associationEnd  
		 */
        REMOVE_MATCHING("removeMatching", Argument.SUBJECT, Argument.PREDICATE, Argument.OBJECT,
                Argument.CONTEXTS),

        /**
		 * @uml.property  name="rESET"
		 * @uml.associationEnd  
		 */
        RESET("reset"),

        /**
		 * @uml.property  name="gET_CLOSURE_STATUS"
		 * @uml.associationEnd  
		 */
        GET_CLOSURE_STATUS("getClosureStatus", ImmutableSet.of(ResponseType.CLOSURE_STATUS)),

        /**
		 * @uml.property  name="uPDATE_CLOSURE"
		 * @uml.associationEnd  
		 */
        UPDATE_CLOSURE("updateClosure"),

        /**
		 * @uml.property  name="cLEAR_CLOSURE"
		 * @uml.associationEnd  
		 */
        CLEAR_CLOSURE("clearClosure"),

        /**
		 * @uml.property  name="eND"
		 * @uml.associationEnd  
		 */
        END("end", Argument.COMMIT);

        /**
		 * @uml.property  name="name"
		 */
        private final String name;

        /**
		 * @uml.property  name="arguments"
		 */
        private final Set<Argument<?>> arguments;

        /**
		 * @uml.property  name="responseTypes"
		 */
        private final Set<ResponseType<?>> responseTypes;

        private Command(final String name, final Argument<?>... arguments)
        {
            this(name, Collections.<ResponseType<?>>emptySet(), arguments);
        }

        private Command(final String name,
                final Iterable<? extends ResponseType<?>> responseTypes,
                final Argument<?>... arguments)
        {
            this.name = name;
            this.arguments = ImmutableSet.copyOf(arguments);
            this.responseTypes = ImmutableSet.copyOf(responseTypes);
        }

        /**
		 * @return
		 * @uml.property  name="name"
		 */
        public String getName()
        {
            return this.name;
        }

        /**
		 * @return
		 * @uml.property  name="arguments"
		 */
        public Set<Argument<?>> getArguments()
        {
            return this.arguments;
        }

        /**
		 * @return
		 * @uml.property  name="responseTypes"
		 */
        public Set<ResponseType<?>> getResponseTypes()
        {
            return this.responseTypes;
        }

        public static Command fromName(final String name)
        {
            for (final Command request : values()) {
                if (name.equals(request.getName())) {
                    return request;
                }
            }
            throw new IllegalArgumentException("Invalid request name: " + name);
        }

    }

}
