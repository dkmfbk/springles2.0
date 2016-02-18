package eu.fbk.dkm.internal.util;

import com.google.common.base.Objects;

import org.openrdf.model.ValueFactory;
import org.openrdf.rio.ParseErrorListener;
import org.openrdf.rio.ParseLocationListener;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.helpers.ParseErrorLogger;

public final class RDFParseOptions implements Cloneable
{

    private static final ParserConfig DEFAULT_PARSER_CONFIG = new ParserConfig();

    /**
	 * @uml.property  name="format"
	 * @uml.associationEnd  
	 */
    private RDFFormat format;

    /**
	 * @uml.property  name="baseURI"
	 */
    private String baseURI;

    /**
	 * @uml.property  name="valueFactory"
	 * @uml.associationEnd  
	 */
    private ValueFactory valueFactory;

   

    /**
	 * @uml.property  name="preserveBNodeIDs"
	 */
    private boolean preserveBNodeIDs;

   
    /**
	 * @uml.property  name="errorListener"
	 * @uml.associationEnd  
	 */
    private ParseErrorListener errorListener;

    /**
	 * @uml.property  name="locationListener"
	 * @uml.associationEnd  
	 */
    private ParseLocationListener locationListener;

    /**
	 * @uml.property  name="uncompressEnabled"
	 */
    private boolean uncompressEnabled;

    public RDFParseOptions()
    {
        this(null, "", null);
    }

    public RDFParseOptions(final RDFFormat format)
    {
        this(format, "", null);
    }

    public RDFParseOptions(final RDFFormat format, final String baseURI)
    {
        this(format, baseURI, null);
    }

    public RDFParseOptions(final RDFFormat format, final String baseURI,
            final ValueFactory valueFactory)
    {
        this(format, baseURI, valueFactory, DEFAULT_PARSER_CONFIG);
    }

    public RDFParseOptions(final RDFFormat format, final String baseURI,
            final ValueFactory valueFactory, final ParserConfig parserConfig)
    {
        final ParserConfig config = Objects.firstNonNull(parserConfig, DEFAULT_PARSER_CONFIG);

        this.format = format;
        this.baseURI = baseURI;
        this.valueFactory = valueFactory;

       
        this.preserveBNodeIDs = config.isPreserveBNodeIDs();
    
        this.errorListener = new ParseErrorLogger();
        this.locationListener = null;
        this.uncompressEnabled = true;
    }

    /**
	 * @return
	 * @uml.property  name="format"
	 */
    public RDFFormat getFormat()
    {
        return this.format;
    }

    /**
	 * @param format
	 * @uml.property  name="format"
	 */
    public void setFormat(final RDFFormat format)
    {
        this.format = format;
    }

    /**
	 * @return
	 * @uml.property  name="baseURI"
	 */
    public String getBaseURI()
    {
        return this.baseURI;
    }

    /**
	 * @param baseURI
	 * @uml.property  name="baseURI"
	 */
    public void setBaseURI(final String baseURI)
    {
        this.baseURI = baseURI;
    }

    /**
	 * @return
	 * @uml.property  name="valueFactory"
	 */
    public ValueFactory getValueFactory()
    {
        return this.valueFactory;
    }

    /**
	 * @param valueFactory
	 * @uml.property  name="valueFactory"
	 */
    public void setValueFactory(final ValueFactory valueFactory)
    {
        this.valueFactory = valueFactory;
    }

   

   
    /**
	 * @return
	 * @uml.property  name="preserveBNodeIDs"
	 */
    public boolean isPreserveBNodeIDs()
    {
        return this.preserveBNodeIDs;
    }

    /**
	 * @param preserveBNodeIDs
	 * @uml.property  name="preserveBNodeIDs"
	 */
    public void setPreserveBNodeIDs(final boolean preserveBNodeIDs)
    {
        this.preserveBNodeIDs = preserveBNodeIDs;
    }

    

   
   

    /**
	 * @return
	 * @uml.property  name="errorListener"
	 */
    public ParseErrorListener getErrorListener()
    {
        return this.errorListener;
    }

    /**
	 * @param errorListener
	 * @uml.property  name="errorListener"
	 */
    public void setErrorListener(final ParseErrorListener errorListener)
    {
        this.errorListener = errorListener;
    }

    /**
	 * @return
	 * @uml.property  name="locationListener"
	 */
    public ParseLocationListener getLocationListener()
    {
        return this.locationListener;
    }

    /**
	 * @param locationListener
	 * @uml.property  name="locationListener"
	 */
    public void setLocationListener(final ParseLocationListener locationListener)
    {
        this.locationListener = locationListener;
    }

    /**
	 * @return
	 * @uml.property  name="uncompressEnabled"
	 */
    public boolean isUncompressEnabled()
    {
        return this.uncompressEnabled;
    }

    /**
	 * @param uncompressEnabled
	 * @uml.property  name="uncompressEnabled"
	 */
    public void setUncompressEnabled(final boolean uncompressEnabled)
    {
        this.uncompressEnabled = uncompressEnabled;
    }

    public RDFParser createParser()
    {
        final RDFParser parser = RDFParserRegistry.getInstance().get(this.format).getParser();
        configure(parser);
        return parser;
    }

    public void configure(final RDFParser parser)
    {
       
        parser.setPreserveBNodeIDs(this.preserveBNodeIDs);
        
        if (this.valueFactory != null) {
            parser.setValueFactory(this.valueFactory);
        }
        if (this.errorListener != null) {
            parser.setParseErrorListener(this.errorListener);
        }
        if (this.locationListener != null) {
            parser.setParseLocationListener(this.locationListener);
        }
    }

    @Override
    public RDFParseOptions clone()
    {
        try {
            return (RDFParseOptions) super.clone();
        } catch (final CloneNotSupportedException ex) {
            throw new Error("Unexpected exception", ex);
        }
    }

    @Override
    public boolean equals(final Object object)
    {
        if (object == this) {
            return true;
        }
        if (object == null || !(object instanceof RDFParseOptions)) {
            return false;
        }
        final RDFParseOptions other = (RDFParseOptions) object;
        return Objects.equal(this.format, other.format)
                && Objects.equal(this.baseURI, other.baseURI)
                && this.valueFactory == other.valueFactory
              
                && this.preserveBNodeIDs == other.preserveBNodeIDs
               
                && this.uncompressEnabled == other.uncompressEnabled;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(this.format, this.baseURI,
                System.identityHashCode(this.valueFactory), 
                this.preserveBNodeIDs, 
                this.uncompressEnabled);
    }

}
