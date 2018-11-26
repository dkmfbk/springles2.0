package eu.fbk.dkm.internal.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
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
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.CharSink;
import com.google.common.io.CharSource;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;

import org.eclipse.rdf4j.common.io.GZipUtil;
import org.eclipse.rdf4j.common.io.ZipUtil;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFParserRegistry;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFWriterRegistry;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;

public abstract class RDFSource<E extends Exception>
{

    public static RDFSource<RuntimeException> wrap(final Iterable<Statement> statements)
    {
        return RDFSource.wrap(statements, Collections.<String, String>emptyMap());
    }

    public static RDFSource<RuntimeException> wrap(final Iterable<Statement> statements,
            final Map<String, String> namespaces)
    {
        return new RDFSource<RuntimeException>() {

            @Override
            public void streamTo(final RDFHandler handler)
                    throws RuntimeException, RDFHandlerException
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
                editedOptions.setFormat(Rio.getParserFormatForFileName(file.getName()).get());
            }
        }

        return new RDFDeserializerSource(Files.asByteSource(file), editedOptions);
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

    public static RDFSource<RDFParseException> deserializeFrom(final ByteSource source,
            final RDFParseOptions options)
    {
        return new RDFDeserializerSource(source, options);
    }

    public static RDFSource<RDFParseException> deserializeFrom(final CharSource source,
            final RDFParseOptions options)
    {
        return new RDFDeserializerSource(source, options);
    }

    @SuppressWarnings("unchecked")
    public static <E extends Exception> RDFSource<E> concat(
            final RDFSource<? extends E>... sources)
    {
        return RDFSource.concat(Arrays.asList(sources));
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
        return this.setContexts(Iterables.toArray(contexts, Resource.class));
    }

    public RDFSource<E> filter(final Predicate<? super Statement> statementPredicate)
    {
        return this.filter(statementPredicate, null);
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
        return this.transform(statementFunction, null);
    }

    public RDFSource<E> transform(final Function<? super Statement, //
            ? extends Iterable<? extends Statement>> statementFunction,
            final Function<? super Namespace, ? extends Namespace> namespaceFunction)
    {
        final RDFSource<E> thisSource = this;
        return new RDFSource<E>() {

            @Override
            public void streamTo(final RDFHandler handler) throws E, RDFHandlerException
            {
                thisSource.streamTo(
                        RDFHandlers.transform(handler, statementFunction, namespaceFunction));
            }
        };
    }

    public abstract void streamTo(final RDFHandler handler) throws E, RDFHandlerException;

    public void streamTo(final Collection<Statement> statements) throws E
    {
        this.streamTo(statements, Maps.<String, String>newHashMap());
    }

    public void streamTo(final Collection<Statement> statements,
            final Map<String, String> namespaces) throws E
    {
        try {
            this.streamTo(new StatementCollector(statements, namespaces));
        } catch (final RDFHandlerException ex) {
            throw new Error("Unexpected exception", ex);
        }
    }

    public Model streamToGraph() throws E
    {
        final Model graph = new LinkedHashModel();
        this.streamTo(graph);
        return graph;
    }

    public StatementCollector streamToCollector() throws E
    {
        try {
            final StatementCollector collector = new StatementCollector();
            this.streamTo(collector);
            return collector;
        } catch (final RDFHandlerException ex) {
            throw new Error("Unexpected exception", ex);
        }
    }

    public Map<String, String> streamToNamespaceMap() throws E
    {
        try {
            final Map<String, String> namespaces = Maps.newHashMap();
            this.streamTo(new AbstractRDFHandler() {

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
            this.streamTo(rdfWriter);

        } catch (final RDFHandlerException ex) {
            if (ex.getCause() instanceof IOException) {
                throw (IOException) ex.getCause();
            }
            throw new Error("Unexpected exception", ex);
        }
    }

    public void serializeTo(final RDFFormat format, final OutputStream stream)
            throws E, IOException
    {
        this.serializeTo(RDFWriterRegistry.getInstance().get(format).get().getWriter(stream));
    }

    public void serializeTo(final RDFFormat format, final Writer writer) throws E, IOException
    {
        this.serializeTo(RDFWriterRegistry.getInstance().get(format).get().getWriter(writer));
    }

    public void serializeTo(final RDFFormat format, final File file) throws E, IOException
    {
        this.serializeTo(format, Files.asByteSink(file, FileWriteMode.APPEND).openStream());
    }

    public void serializeTo(final RDFFormat format, final ByteSink sink) throws E, IOException
    {
        try (OutputStream output = sink.openBufferedStream()) {
            this.serializeTo(format, output);
        }
    }

    public void serializeTo(final RDFFormat format, final CharSink sink) throws E, IOException
    {
        try (Writer output = sink.openBufferedStream()) {
            this.serializeTo(format, output);
        }
    }

    public byte[] toByteArray(final RDFFormat format) throws E
    {
        return this.toByteArray(format, null);
    }

    public byte[] toByteArray(final RDFFormat format, final Charset charset) throws E
    {
        try {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            if (charset == null) {
                this.serializeTo(format, stream);
            } else {
                this.serializeTo(format, new OutputStreamWriter(stream, charset));
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
            this.serializeTo(format, writer);
            return writer.toString();

        } catch (final IOException ex) {
            throw new Error("Unexpected exception", ex);
        }
    }

    @Override
    public String toString()
    {
        try {
            return this.toString(RDFFormat.TRIG);
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * @author calabrese
     */
    private static class RDFDeserializerSource extends RDFSource<RDFParseException>
    {

        private final Object source;

        private final RDFParseOptions options;

        public RDFDeserializerSource(final Object source, final RDFParseOptions options)
        {
            Preconditions.checkNotNull(source);
            Preconditions.checkNotNull(options);

            this.source = source;
            this.options = options;
        }

        @Override
        public void streamTo(final RDFHandler handler)
                throws RDFParseException, RDFHandlerException
        {
            try {
                this.processSource(handler, this.source, this.options.getFormat());

            } catch (final IOException ex) {
                throw new RDFParseException(ex);
            }
        }

        private void processSource(final RDFHandler handler, final Object source,
                final RDFFormat format) throws IOException, RDFHandlerException, RDFParseException
        {
            if (source instanceof ByteSource) {
                this.processByteSource(handler, (ByteSource) source, format);

            } else if (source instanceof CharSource) {
                this.processCharSource(handler, (CharSource) source, format);

            } else if (source instanceof URL) {
                this.processURL(handler, (URL) source, format);

            } else if (source instanceof Reader) {
                this.processReader(handler, (Reader) source, format);

            } else if (source instanceof InputStream) {
                this.processStream(handler, (InputStream) source, format);

            }
        }

        private void processByteSource(final RDFHandler handler, final ByteSource source,
                final RDFFormat format) throws IOException, RDFHandlerException, RDFParseException
        {
            try (InputStream stream = source.openBufferedStream()) {
                this.processStream(handler, stream, format);
            }
        }

        private void processCharSource(final RDFHandler handler, final CharSource source,
                final RDFFormat format) throws IOException, RDFHandlerException, RDFParseException
        {
            try (Reader reader = source.openBufferedStream()) {
                this.processReader(handler, reader, format);
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
                    final String mimeType = connection.getContentType();
                    if (mimeType != null) {
                        final int semiColonIdx = mimeType.indexOf(';');
                        final String mt = semiColonIdx < 0 ? mimeType
                                : mimeType.substring(0, semiColonIdx);
                        chosenFormat = Rio.getParserFormatForMIMEType(mt).orElse(null);
                    }
                    if (chosenFormat == null) {
                        chosenFormat = Rio.getParserFormatForFileName(url.getPath()).orElse(null);
                    }
                }

                this.processSource(handler, stream, chosenFormat);

            } finally {
                stream.close();
            }
        }

        private void processReader(final RDFHandler handler, final Reader reader,
                final RDFFormat format) throws IOException, RDFHandlerException, RDFParseException
        {
            final RDFParser parser = RDFParserRegistry.getInstance().get(format).get().getParser();
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
                final RDFParser parser = RDFParserRegistry.getInstance().get(format).get()
                        .getParser();
                parser.setRDFHandler(handler);
                this.options.configure(parser);
                parser.parse(actualStream, this.options.getBaseURI());

            } else if (isGZip && this.options.isUncompressEnabled()) {
                this.processStream(handler, new GZIPInputStream(actualStream), format);

            } else if (isZip && this.options.isUncompressEnabled()) {
                this.processZip(handler, new ZipInputStream(actualStream), format);
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

                final RDFFormat actualFormat = Rio.getParserFormatForFileName(entry.getName())
                        .get();

                try {
                    // Prevent parser (Xerces) from closing the input stream.
                    this.processStream(handler, new FilterInputStream(stream) {

                        @Override
                        public void close()
                        {
                        }

                    }, actualFormat);

                } catch (final RDFParseException ex) {
                    // Wrap so to provide information about the source of the error.
                    throw (RDFParseException) new RDFParseException(
                            ex.getMessage() + " in " + entry.getName(), ex.getLineNumber(),
                            ex.getColumnNumber()).initCause(ex);

                } finally {
                    stream.closeEntry();
                }

            }
        }

    }

}
