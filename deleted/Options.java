package eu.fbk.dkm.internal.springles.protocol;

import java.io.Serializable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.openrdf.model.Literal;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.ListBindingSet;

import eu.fbk.dkm.springles.TransactionMode;

public final class Options implements Serializable
{

    private static final long serialVersionUID = 4231078922448386194L;

    /**
	 * @uml.property  name="transactionName"
	 */
    private final String transactionName;

    /**
	 * @uml.property  name="transactionMode"
	 * @uml.associationEnd  multiplicity="(1 1)"
	 */
    private final TransactionMode transactionMode;

    /**
	 * @uml.property  name="autoCommit"
	 */
    private final boolean autoCommit;

    public Options(final String transactionName, final TransactionMode transactionMode,
            final boolean autoCommit)
    {
        Preconditions.checkNotNull(transactionName);
        Preconditions.checkNotNull(transactionMode);
        this.transactionName = transactionName;
        this.transactionMode = transactionMode;
        this.autoCommit = autoCommit;
    }

    /**
	 * @return
	 * @uml.property  name="transactionName"
	 */
    public String getTransactionName()
    {
        return this.transactionName;
    }

    /**
	 * @return
	 * @uml.property  name="transactionMode"
	 */
    public TransactionMode getTransactionMode()
    {
        return this.transactionMode;
    }

    /**
	 * @return
	 * @uml.property  name="autoCommit"
	 */
    public boolean isAutoCommit()
    {
        return this.autoCommit;
    }

    @Override
    public boolean equals(final Object object)
    {
        if (object == this) {
            return true;
        }
        if (object == null || !(object instanceof Options)) {
            return false;
        }
        final Options other = (Options) object;
        return other.transactionName.equals(this.transactionName)
                && other.transactionMode.equals(this.transactionMode)
                && other.autoCommit == this.autoCommit;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(this.transactionName, this.transactionMode, this.autoCommit);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this).add("transactionName", this.transactionName)
                .add("transactionMode", this.transactionMode).add("autoCommit", this.autoCommit)
                .toString();
    }

    public BindingSet toBindings()
    {
        return new ListBindingSet(
                ImmutableList.of("tx-name", "tx-mode", "tx-auto-commit"),
                new LiteralImpl(this.transactionName, XMLSchema.STRING), //
                new LiteralImpl(this.transactionMode.toString(), XMLSchema.STRING),
                new LiteralImpl(Boolean.toString(this.autoCommit), XMLSchema.BOOLEAN));
    }

    public static Options fromBindings(final BindingSet bindings)
    {
        try {
            String transactionID = null;
            TransactionMode transactionMode = null;
            boolean autoCommit = true;

            for (final Binding binding : bindings) {
                if ("tx-name".equals(binding.getName())) {
                    transactionID = ((Literal) binding.getValue()).getLabel();
                } else if ("tx-mode".equals(binding.getName())) {
                    transactionMode = TransactionMode.valueOf(((Literal) binding.getValue())
                            .getLabel());
                } else if ("tx-auto-commit".equals(binding.getName())) {
                    autoCommit = ((Literal) binding.getValue()).booleanValue();
                } else {
                    throw new IllegalArgumentException("Unrecognized option: " + binding.getName());
                }
            }

            return new Options(transactionID, transactionMode, autoCommit);

        } catch (final Throwable ex) {
            throw new IllegalArgumentException("Invalid transaction specification", ex);
        }
    }

}
