package eu.fbk.dkm.internal.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;

import org.openrdf.model.Graph;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterRegistry;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.helpers.StatementCollector;

import info.aduna.io.GZipUtil;
import info.aduna.io.ZipUtil;

public abstract class RDFSource<E extends Exception>
{

    public static RDFSource<RuntimeException> wrap(final Iterable<Statement> statements)
    {
        return wrap(statements, Collections.<String, String>emptyMap());
    }

    public static RDFSource<RuntimeException> wrap(final Iterable<Statement> statements,
            final Map<String, String> namespaces)
    {
        return new RDFSource<RuntimeException>() {

            @Override
            public void streamTo(final RDFHandler handler) throws RuntimeException,
                    RDFHandlerException
            {
                handler.startRDF();
                for (final Map.Entry<String, String> entry : namespaces.entrySet()) {
                    handler.handleNamespace(entry.getKey(), entry.getValue());
                }
                for (final Statement statement : statements) {
                    handler.handleStatement(statement);
                }
                handler.endRDF();
            }

        };
    }

    public static RDFSource<RDFParseException> deserializeFrom(final InputStream stream,
            final RDFParseOptions options)
    {
        return new RDFDeserializerSource(stream, options);
    }

    public static RDFSource<RDFParseException> deserializeFrom(final Reader reader,
            final RDFParseOptions options)
    {
        return new RDFDeserializerSource(reader, options);
    }

    public static RDFSource<RDFParseException> deserializeFrom(final File file,
            final RDFParseOptions options)
    {
        RDFParseOptions editedOptions = options;
        if (editedOptions.getBaseURI() == null || editedOptions.getFormat() == null) {
            editedOptions = editedOptions.clone();
            if (editedOptions.getBaseURI() == null) {
                editedOptions.setBaseURI(file.toURI().toString());
            }
            if (editedOptions.getFormat() == null) {
                editedOptions.setFormat(Rio.getParserFormatForFileName(file.getName()));
            }
        }

        return new RDFDeserializerSource(Files.newInputStreamSupplier(file), editedOptions);
    }

    public static RDFSource<RDFParseException> deserializeFrom(final URL url,
            final RDFParseOptions options)
    {
        RDFParseOptions editedOptions = options;
        if (editedOptions.getBaseURI() == null) {
            editedOptions = editedOptions.clone();
            editedOptions.setBaseURI(url.toExternalForm());
        }

        return new RDFDeserializerSource(url, editedOptions);
    }

    public static RDFSource<RDFParseException> deserializeFrom(final InputSupplier<?> supplier,
            final RDFParseOptions options)
    {
        return new RDFDeserializerSource(supplier, options);
    }

    public static <E extends Exception> RDFSource<E> concat(
            final RDFSource<? extends E>... sources)
    {
        return concat(Arrays.asList(sources));
    }

    public static <E extends Exception> RDFSource<E> concat(
            final Iterable<? extends RDFSource<? extends E>> sources)
    {
        return new RDFSource<E>() {

            @Override
            public void streamTo(final RDFHandler handler) throws E, RDFHandlerException
            {
                handler.startRDF();

                final RDFHandler filteredHandler = RDFHandlers.filter(handler, false, true, true,
                        false);
                for (final RDFSource<? extends E> source : sources) {
                    source.streamTo(filteredHandler);
                }

                handler.endRDF();
            }

        };
    }

    public RDFSource<E> setContexts(final Resource... contexts)
    {
        if (contexts.length == 0) {
            return this;

        } else {
            final RDFSource<E> thisSource = this;
            return new RDFSource<E>() {

                @Override
                public void streamTo(final RDFHandler handler) throws E, RDFHandlerException
                {
                    thisSource.streamTo(RDFHandlers.setContexts(handler, contexts));
                }
            };
        }
    }

    public RDFSource<E> setContexts(final Iterable<Resource> contexts)
    {
        return setContexts(Iterables.toArray(contexts, Resource.class));
    }

    public RDFSource<E> filter(final Predicate<? super Statement> statementPredicate)
    {
        return filter(statementPredicate, null);
    }

    public RDFSource<E> filter(final Predicate<? super Statement> statementPredicate,
            final Predicate<? super Namespace> namespacePredicate)
    {
        final RDFSource<E> thisSource = this;
        return new RDFSource<E>() {

            @Override
            public void streamTo(final RDFHandler handler) throws E, RDFHandlerException
            {
                thisSource.streamTo(RDFHandlers.filter(handler, true, namespacePredicate,
                        statementPredicate, true));
            }

        };
    }

    public RDFSource<E> transform(final Function<? super Statement, //
            ? extends Iterable<? extends Statement>> statementFunction)
    {
        return transform(statementFunction, null);
    }

    public RDFSource<E> transform(
            final Function<? super Statement, //
            ? extends Iterable<? extends Statement>> statementFunction,
            final Function<? super Namespace, ? extends Namespace> namespaceFunction)
    {
        final RDFSource<E> thisSource = this;
        return new RDFSource<E>() {

            @Override
            public void streamTo(final RDFHandler handler) throws E, RDFHandlerException
            {
                thisSource.streamTo(RDFHandlers.transform(handler, statementFunction,
                        namespaceFunction));
            }
        };
    }

    public abstract void streamTo(final RDFHandler handler) throws E, RDFHandlerException;

    public void streamTo(final Collection<Statement> statements) throws E
    {
        streamTo(statements, Maps.<String, String>newHashMap());
    }

    public void streamTo(final Collection<Statement> statements,
            final Map<String, String> namespaces) throws E
    {
        try {
            streamTo(new StatementCollector(statements, namespaces));
        } catch (final RDFHandlerException ex) {
            throw new Error("Unexpected exception", ex);
        }
    }

    public Graph streamToGraph() throws E
    {
        final Graph graph = new GraphImpl();
        streamTo(graph);
        return graph;
    }

    public StatementCollector streamToCollector() throws E
    {
        try {
            final StatementCollector collector = new StatementCollector();
            streamTo(collector);
            return collector;
        } catch (final RDFHandlerException ex) {
            throw new Error("Unexpected exception", ex);
        }
    }

    public Map<String, String> streamToNamespaceMap() throws E
    {
        try {
            final Map<String, String> namespaces = Maps.newHashMap();
            streamTo(new RDFHandlerBase() {

                @Override
                public void handleNamespace(final String prefix, final String uri)
                {
                    namespaces.put(prefix, uri);
                }

            });
            return namespaces;

        } catch (final RDFHandlerException ex) {
            throw new Error("Unexpected exception", ex);
        }
    }

    public void serializeTo(final RDFWriter rdfWriter) throws E, IOException
    {
        try {
            streamTo(rdfWriter);

        } catch (final RDFHandlerException ex) {
            if (ex.getCause() instanceof IOException) {
                throw (IOException) ex.getCause();
            }
            throw new Error("Unexpected exception", ex);
        }
    }

    public void serializeTo(final RDFFormat format, final OutputStream stream) throws E,
            IOException
    {
        serializeTo(RDFWriterRegistry.getInstance().get(format).getWriter(stream));
    }

    public void serializeTo(final RDFFormat format, final Writer writer) throws E, IOException
    {
        serializeTo(RDFWriterRegistry.getInstance().get(format).getWriter(writer));
    }

    public void serializeTo(final RDFFormat format, final File file) throws E, IOException
    {
        serializeTo(format, Files.newOutputStreamSupplier(file));
    }

    public void serializeTo(final RDFFormat format, final OutputSupplier<?> supplier) throws E,
            IOException
    {
        final Object output = supplier.getOutput();

        if (output instanceof OutputStream) {
            try {
                serializeTo(format, (OutputStream) output);
            } finally {
                ((OutputStream) output).close();
            }

        } else if (output instanceof Writer) {
            try {
                serializeTo(format, (Writer) output);
            } finally {
                ((Writer) output).close();
            }

        } else {
            Preconditions.checkNotNull(output);
            throw new UnsupportedOperationException("Cannot serialize to "
                    + output.getClass().getName());
        }
    }

    public byte[] toByteArray(final RDFFormat format) throws E
    {
        return toByteArray(format, null);
    }

    public byte[] toByteArray(final RDFFormat format, final Charset charset) throws E
    {
        try {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            if (charset == null) {
                serializeTo(format, stream);
            } else {
                serializeTo(format, new OutputStreamWriter(stream, charset));
            }
            return stream.toByteArray();

        } catch (final IOException ex) {
            throw new Error("Unexpected exception", ex);
        }
    }

    public String toString(final RDFFormat format) throws E
    {
        try {
            final StringWriter writer = new StringWriter();
            serializeTo(format, writer);
            return writer.toString();

        } catch (final IOException ex) {
            throw new Error("Unexpected exception", ex);
        }
    }

    @Override
    public String toString()
    {
        try {
            return toString(RDFFormat.TRIG);
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
	 * @author  calabrese
	 */
    private static class RDFDeserializerSource extends RDFSource<RDFParseException>
    {

        private final Object source;

        /**
		 * @uml.property  name="options"
		 * @uml.associationEnd  
		 */
        private final RDFParseOptions options;

        public RDFDeserializerSource(final Object source, final RDFParseOptions options)
        {
            Preconditions.checkNotNull(source);
            Preconditions.checkNotNull(options);

            this.source = source;
            this.options = options;
        }

        @Override
        public void streamTo(final RDFHandler handler) throws RDFParseException,
                RDFHandlerException
        {
            try {
                processSource(handler, this.source, this.options.getFormat());

            } catch (final IOException ex) {
                throw new RDFParseException(ex);
            }
        }

        private void processSource(final RDFHandler handler, final Object source,
                final RDFFormat format) throws IOException, RDFHandlerException, RDFParseException
        {
            if (source instanceof InputSupplier<?>) {
                processSupplier(handler, (InputSupplier<?>) source, format);

            } else if (source instanceof URL) {
                processURL(handler, (URL) source, format);

            } else if (source instanceof Reader) {
                processReader(handler, (Reader) source, format);

            } else if (source instanceof InputStream) {
                processStream(handler, (InputStream) source, format);

            }
        }

        private void processSupplier(final RDFHandler handler, final InputSupplier<?> supplier,
                final RDFFormat format) throws IOException, RDFHandlerException, RDFParseException
        {
            final Object input = supplier.getInput();

            try {
                processSource(handler, input, format);

            } finally {
                ((Closeable) input).close();
            }
        }

        private void processURL(final RDFHandler handler, final URL url, final RDFFormat format)
                throws IOException, RDFHandlerException, RDFParseException
        {
            final URLConnection connection = url.openConnection();

            RDFFormat chosenFormat = format;

            for (final String acceptParam : chosenFormat != null ? chosenFormat.getMIMETypes()
                    : RDFFormat.getAcceptParams(RDFParserRegistry.getInstance().getKeys(), true,
                            null)) {
                connection.addRequestProperty("Accept", acceptParam);
            }

            final InputStream stream = connection.getInputStream();

            try {
                if (chosenFormat == null) {
                    String mimeType = connection.getContentType();
                    if (mimeType != null) {
                        final int semiColonIdx = mimeType.indexOf(';');
                        if (semiColonIdx >= 0) {
                            mimeType = mimeType.substring(0, semiColonIdx);
                        }
                        chosenFormat = Rio.getParserFormatForMIMEType(mimeType);
                    }
                    if (chosenFormat == null) {
                        chosenFormat = Rio.getParserFormatForFileName(url.getPath());
                    }
                }

                processSource(handler, stream, chosenFormat);

            } finally {
                stream.close();
            }
        }

        private void processReader(final RDFHandler handler, final Reader reader,
                final RDFFormat format) throws IOException, RDFHandlerException, RDFParseException
        {
            final RDFParser parser = RDFParserRegistry.getInstance().get(format).getParser();
            parser.setRDFHandler(handler);
            this.options.configure(parser);
            parser.parse(reader, this.options.getBaseURI());
        }

        private void processStream(final RDFHandler handler, final InputStream stream,
                final RDFFormat format) throws IOException, RDFHandlerException, RDFParseException
        {
            final InputStream actualStream = stream.markSupported() ? stream
                    : new BufferedInputStream(stream);

            final boolean isGZip = GZipUtil.isGZipStream(actualStream);
            final boolean isZip = !isGZip && ZipUtil.isZipStream(actualStream);

            if (!isGZip && !isZip) {
                final RDFParser parser = RDFParserRegistry.getInstance().get(format).getParser();
                parser.setRDFHandler(handler);
                this.options.configure(parser);
                parser.parse(actualStream, this.options.getBaseURI());

            } else if (isGZip && this.options.isUncompressEnabled()) {
                processStream(handler, new GZIPInputStream(actualStream), format);

            } else if (isZip && this.options.isUncompressEnabled()) {
                processZip(handler, new ZipInputStream(actualStream), format);
            }
        }

        private void processZip(final RDFHandler handler, final ZipInputStream stream,
                final RDFFormat format) throws IOException, RDFHandlerException, RDFParseException
        {
            while (true) {

                final ZipEntry entry = stream.getNextEntry();
                if (entry == null) {
                    break;
                } else if (entry.isDirectory()) {
                    continue;
                }

                final RDFFormat actualFormat = Rio.getParserFormatForFileName(entry.getName(),
                        format);

                try {
                    // Prevent parser (Xerces) from closing the input stream.
                    processStream(handler, new FilterInputStream(stream) {

                        @Override
                        public void close()
                        {
                        }

                    }, actualFormat);

                } catch (final RDFParseException ex) {
                    // Wrap so to provide information about the source of the error.
                    throw (RDFParseException) new RDFParseException(ex.getMessage() + " in "
                            + entry.getName(), ex.getLineNumber(), ex.getColumnNumber())
                            .initCause(ex);

                } finally {
                    stream.closeEntry();
                }

            }
        }

    }

}
