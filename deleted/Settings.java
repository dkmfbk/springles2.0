package eu.fbk.dkm.internal.springles.protocol;

import java.io.Serializable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.ListBindingSet;

import eu.fbk.dkm.springles.InferenceMode;

public final class Settings implements Serializable
{

    private static final long serialVersionUID = 5303234079561698414L;

    /**
	 * @uml.property  name="assignedClientID"
	 */
    private final String assignedClientID;

    /**
	 * @uml.property  name="nullContextURI"
	 * @uml.associationEnd  multiplicity="(1 1)"
	 */
    private final URI nullContextURI;

    /**
	 * @uml.property  name="inferredContextPrefix"
	 */
    private final String inferredContextPrefix;

    /**
	 * @uml.property  name="supportedInferenceMode"
	 * @uml.associationEnd  multiplicity="(1 1)"
	 */
    private final InferenceMode supportedInferenceMode;

    /**
	 * @uml.property  name="writable"
	 */
    private final boolean writable;

    public Settings(final String assignedClientID, final URI nullContextURI,
            final String inferredContextPrefix, final InferenceMode supportedInferenceMode,
            final boolean writable)
    {
        Preconditions.checkNotNull(assignedClientID);
        Preconditions.checkNotNull(nullContextURI);
        Preconditions.checkNotNull(inferredContextPrefix);
        Preconditions.checkNotNull(supportedInferenceMode);

        this.assignedClientID = assignedClientID;
        this.nullContextURI = nullContextURI;
        this.inferredContextPrefix = inferredContextPrefix;
        this.supportedInferenceMode = supportedInferenceMode;
        this.writable = writable;
    }

    /**
	 * @return
	 * @uml.property  name="assignedClientID"
	 */
    public String getAssignedClientID()
    {
        return this.assignedClientID;
    }

    /**
	 * @return
	 * @uml.property  name="nullContextURI"
	 */
    public URI getNullContextURI()
    {
        return this.nullContextURI;
    }

    /**
	 * @return
	 * @uml.property  name="inferredContextPrefix"
	 */
    public String getInferredContextPrefix()
    {
        return this.inferredContextPrefix;
    }

    /**
	 * @return
	 * @uml.property  name="supportedInferenceMode"
	 */
    public InferenceMode getSupportedInferenceMode()
    {
        return this.supportedInferenceMode;
    }

    /**
	 * @return
	 * @uml.property  name="writable"
	 */
    public boolean isWritable()
    {
        return this.writable;
    }

    @Override
    public boolean equals(final Object object)
    {
        if (object == this) {
            return true;
        }
        if (object == null || !(object instanceof Settings)) {
            return false;
        }
        final Settings other = (Settings) object;
        return other.nullContextURI.equals(this.nullContextURI)
                && other.inferredContextPrefix.equals(this.inferredContextPrefix)
                && other.supportedInferenceMode == this.supportedInferenceMode
                && other.writable == this.writable;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(this.nullContextURI, this.inferredContextPrefix,
                this.supportedInferenceMode, this.writable);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(Settings.class).add("nullGraphURI", this.nullContextURI)
                .add("inferredGraphPrefix", this.inferredContextPrefix)
                .add("supportedInferenceMode", this.supportedInferenceMode)
                .add("writable", this.writable).toString();
    }

    public BindingSet toBindings()
    {
        return new ListBindingSet(ImmutableList.of("message", "assignedClientID", "nullGraphURI",
                "inferredGraphPrefix", "supportedInferenceMode", "writable"),
                new LiteralImpl(this.assignedClientID, XMLSchema.STRING), //
                this.nullContextURI, //
                new LiteralImpl(this.inferredContextPrefix.toString(), XMLSchema.STRING), //
                new LiteralImpl(this.supportedInferenceMode.toString(), XMLSchema.STRING),
                new LiteralImpl(Boolean.toString(this.writable), XMLSchema.BOOLEAN));
    }

    public static Settings fromBindings(final BindingSet bindings)
    {
        try {
            String assignedClientID = null;
            URI nullGraphURI = null;
            String inferredGraphPrefix = null;
            InferenceMode supportedInferenceMode = null;
            Boolean writable = null;

            for (final Binding binding : bindings) {
                if ("assignedClientID".equals(binding.getName())) {
                    assignedClientID = binding.getValue().stringValue();
                } else if ("nullGraphURI".equals(binding.getName())) {
                    nullGraphURI = (URI) binding.getValue();
                } else if ("inferredGraphPrefix".equals(binding.getName())) {
                    inferredGraphPrefix = binding.getValue().stringValue();
                } else if ("supportedInferenceMode".equals(binding.getName())) {
                    supportedInferenceMode = InferenceMode.valueOf(binding.getValue()
                            .stringValue());
                } else if ("writable".equals(binding.getName())) {
                    writable = ((Literal) binding.getValue()).booleanValue();
                } else {
                    throw new IllegalArgumentException("Unrecognized capability: "
                            + binding.getName());
                }
            }

            return new Settings(assignedClientID, nullGraphURI, inferredGraphPrefix,
                    supportedInferenceMode, writable);

        } catch (final Throwable ex) {
            throw new IllegalArgumentException("Invalid client settings message", ex);
        }
    }

}
