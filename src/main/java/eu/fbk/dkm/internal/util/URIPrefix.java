package eu.fbk.dkm.internal.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;

public final class URIPrefix implements Serializable, Comparable<URIPrefix>
{

    private static final long serialVersionUID = -5453141226228571058L;

    private final String prefix;

    private final boolean isFullURI;

    private transient URI fullURI;

    private transient Predicate<Value> valueMatcher;

    private transient Predicate<Statement> contextMatcher;

    public static URIPrefix from(final URI uri)
    {
        return new URIPrefix(uri, uri.stringValue());
    }

    public static URIPrefix from(final String string)
    {
        if (string.endsWith("*")) {
            return new URIPrefix(null, string.substring(0, string.length() - 1));
        } else {
            return new URIPrefix(new URIImpl(string), string);
        }
    }

    private URIPrefix(final URI uri, final String prefix)
    {
        this.fullURI = uri;
        this.isFullURI = uri != null;
        this.prefix = prefix;
        this.valueMatcher = null;
        this.contextMatcher = null;
    }

    public String getPrefix()
    {
        return this.prefix;
    }

    public boolean isFullURI()
    {
        return this.isFullURI;
    }

    public URI asFullURI()
    {
        Preconditions.checkState(this.isFullURI);
        return this.fullURI;
    }

    public boolean matches(final String string)
    {
        return string != null
                && (this.isFullURI ? string.equals(this.prefix) : string.startsWith(this.prefix));
    }

    public boolean matches(final Value value)
    {
        return value instanceof URI
                && (this.isFullURI ? value.equals(this.fullURI) : value.stringValue().startsWith(
                        this.prefix));
    }

    public boolean matches(final URI uri)
    {
        return uri != null
                && (this.isFullURI ? uri.equals(this.fullURI) : uri.stringValue().startsWith(
                        this.prefix));
    }

    public Predicate<Value> valueMatcher()
    {
        // Create the predicate at first access. No synchronization: doesn't matter if multiple
        // instances are created due to concurrent invocations of the method.
        if (this.valueMatcher == null) {
            if (this.isFullURI) {
                this.valueMatcher = new Predicate<Value>() {

                    @Override
                    public boolean apply(final Value value)
                    {
                        return URIPrefix.this.fullURI.equals(value);
                    }

                };
            } else {
                this.valueMatcher = new Predicate<Value>() {

                    @Override
                    public boolean apply(final Value value)
                    {
                        return value instanceof URI
                                && value.stringValue().startsWith(URIPrefix.this.prefix);
                    }

                };
            }
        }

        // Return the cached predicate.
        return this.valueMatcher;
    }

    public Predicate<Statement> contextMatcher()
    {
        // Create the predicate at first access. No synchronization: doesn't matter if multiple
        // instances are created due to concurrent invocations of the method.
        if (this.contextMatcher == null) {
            if (this.isFullURI) {
                this.contextMatcher = new Predicate<Statement>() {

                    @Override
                    public boolean apply(final Statement statement)
                    {
                        final Resource context = statement.getContext();
                        return URIPrefix.this.fullURI.equals(context);
                    }

                };
            } else {
                this.contextMatcher = new Predicate<Statement>() {

                    @Override
                    public boolean apply(final Statement statement)
                    {
                        final Resource context = statement.getContext();
                        return context instanceof URI
                                && context.stringValue().startsWith(URIPrefix.this.prefix);
                    }

                };
            }
        }

        // Return the cached predicate.
        return this.contextMatcher;
    }

    @Override
    public int compareTo(final URIPrefix other)
    {
        int result = this.prefix.compareTo(other.prefix);
        if (result == 0) {
            if (this.isFullURI && !other.isFullURI) {
                result = -1;
            } else if (!this.isFullURI && other.isFullURI) {
                result = 1;
            }
        }
        return result;
    }

    @Override
    public boolean equals(final Object object)
    {
        if (object == this) {
            return true;
        }
        if (object == null || !(object instanceof URIPrefix)) {
            return false;
        }
        final URIPrefix other = (URIPrefix) object;
        return other.prefix.equals(this.prefix) && other.isFullURI == this.isFullURI;
    }

    @Override
    public int hashCode()
    {
        return this.prefix.hashCode() ^ (isFullURI() ? 1 : -1);
    }

    @Override
    public String toString()
    {
        return this.isFullURI ? this.prefix : this.prefix + "*";
    }

    private void readObject(final ObjectInputStream ois) throws IOException,
            ClassNotFoundException
    {
        ois.defaultReadObject();
        this.fullURI = this.isFullURI ? new URIImpl(this.prefix) : null;
    }

}
