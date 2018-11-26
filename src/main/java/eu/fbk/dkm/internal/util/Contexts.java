package eu.fbk.dkm.internal.util;

import java.util.Iterator;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods and constants for dealing with Sesame contexts.
 */
public final class Contexts
{

    /** Shared log object. */
    private static final Logger LOGGER = LoggerFactory.getLogger(Contexts.class);

    /** A <tt>null</tt> resource array used by methods in this class to denote 'no contexts'. */
    public static final Resource[] NONE = null;

    /** An empty resource array denoting 'unspecified context' in Sesame. */
    public static final Resource[] UNSPECIFIED = new Resource[] {};

    /**
     * Filters a context array removing contexts matching a given predicate. The supplied array is
     * never modified: it is returned unchanged if removal is not necessary, otherwise a new array
     * is returned.
     * 
     * @param contexts
     *            the context array to filter
     * @param removePredicate
     *            the predicate to be satisfied by removed contexts
     * @param warningMessageOnRemove
     *            an optional warning message logged at <tt>WARN</tt> level in case removal
     *            occurs; this parameter is actually a template with an optional placeholder (
     *            <tt>{}</tt>) which is replaced by the number of contexts removed
     * @return the filtered context array, which is {@link #NONE} in case no context remains or
     *         {@link #UNSPECIFIED} in case the supplied array was empty thus denoting the
     *         'unspecified context' case
     */
    public static Resource[] filter(final Resource[] contexts,
            final Predicate<? super Resource> removePredicate,
            @Nullable final String warningMessageOnRemove)
    {
        if (contexts.length == 0) {
            return Contexts.UNSPECIFIED;
        }

        Resource[] filteredContexts = contexts;

        // Filtering loop. Context array cloned only if must be modified. Variable
        // length stores the length of the resulting array.
        int length = 0;
        for (int i = 0; i < filteredContexts.length; ++i) {
            if (!removePredicate.apply(filteredContexts[i])) {
                if (length != i) {
                    filteredContexts[length] = filteredContexts[i];
                }
                ++length;
            } else if (length == i) {
                filteredContexts = filteredContexts.clone();
            }
        }

        if (length < contexts.length && warningMessageOnRemove != null) {
            LOGGER.warn(warningMessageOnRemove, contexts.length - length);
        }

        if (length == 0) {
            return Contexts.NONE;
        }

        if (length < filteredContexts.length) {
            final Resource[] oldContexts = filteredContexts;
            filteredContexts = new Resource[length];
            System.arraycopy(oldContexts, 0, filteredContexts, 0, length);
        }
        return filteredContexts;
    }

    /**
     * Filters a statements <tt>Iterable</tt> removing statements whose context matches the
     * predicate supplied. The method returns an <tt>Iterable</tt> object that wraps the supplied
     * statement <tt>Iterable</tt> and performs filtering as iteration occurs. A warning message
     * is optionall logged if at least a statement is removed.
     * 
     * @param iterable
     *            the statements to filter
     * @param removePredicate
     *            the predicate to be satisfied by contexts of removed statements
     * @param warningMessageOnRemove
     *            an optional warning message logged at <tt>WARN</tt> level in case removal
     *            occurs; this parameter is actually a template with an optional placeholder (
     *            <tt>{}</tt>) which is replaced by the number of statements removed
     * @return a statement <tt>Iterable</tt> that performs filtering during iteration
     */
    public static Iterable<Statement> filter(final Iterable<? extends Statement> iterable,
            final Predicate<? super Resource> removePredicate,
            @Nullable final String warningMessageOnRemove)
    {
        return new Iterable<Statement>() {

            @Override
            public Iterator<Statement> iterator()
            {
                return new AbstractIterator<Statement>() {

                    private final Iterator<? extends Statement> iterator = iterable.iterator();

                    private int skipped = 0;

                    @Override
                    protected Statement computeNext()
                    {
                        while (this.iterator.hasNext()) {

                            final Statement statement = this.iterator.next();
                            final Resource context = statement.getContext();

                            if (removePredicate.apply(context)) {
                                ++this.skipped;
                                continue;
                            }

                            return statement;
                        }

                        // Log only at the end of the iteration to reduce verbosity.
                        if (this.skipped != 0 && warningMessageOnRemove != null) {
                            LOGGER.warn(warningMessageOnRemove, this.skipped);
                        }
                        return endOfData();
                    }

                };
            }

        };
    }

    /**
     * Rewrites the contexts in the supplied array, replacing the null context (<tt>null</tt>)
     * with the supplied context. If the null context is not mentioned, the input array is
     * returned unchanged; otherwise, a new array is produced. In any case, the input array is not
     * changed.
     * 
     * @param contexts
     *            the array of context to be rewritten
     * @param nullContextReplacement
     *            the context to be used in place of the null context
     * @return the rewritten contexts array, which is {@link #UNSPECIFIED} in case the supplied
     *         array was empty thus denoting the 'unspecified contexts' case
     */
    public static Resource[] rewrite(final Resource[] contexts,
            final Resource nullContextReplacement)
    {
        if (contexts.length == 0) {
            return Contexts.UNSPECIFIED;
        }

        Resource[] filteredContexts = contexts;

        boolean cloned = false;
        for (int i = 0; i < filteredContexts.length; ++i) {
            if (filteredContexts[i] == null) {
                if (!cloned) {
                    filteredContexts = filteredContexts.clone();
                    cloned = true;
                }
                filteredContexts[i] = nullContextReplacement;
            }
        }
        return filteredContexts;
    }

    /**
     * Rewrites and filters a context array in a single pass, replacing the null context with a
     * corresponding context and removing contexts matching a supplied predicate. This method
     * efficiently combines the effects of {@link #rewrite(Resource[], Resource)} and
     * {@link #filter(Resource[], Predicate, String)} by operating in a single pass. As in both
     * methods, the supplied array is not modified and is returned unchanged is filtering or
     * rewriting is not required. As in <tt>filter</tt>, a warning message is optionally logged if
     * context removal occurs.
     * 
     * @param contexts
     *            the context array to be processed
     * @param nullContextReplacement
     *            the context to be used in place of the null context
     * @param removePredicate
     *            the predicate to be satisfied by removed contexts
     * @param warningMessageOnRemove
     *            an optional warning message logged at <tt>WARN</tt> level in case context
     *            removal occurs; this parameter is actually a template with an optional
     *            placeholder ( <tt>{}</tt>) which is replaced by the number of contexts removed
     * @return the filtered and rewritten context array, which is {@link #NONE} in case no context
     *         remains or {@link #UNSPECIFIED} in case the supplied array was empty thus denoting
     *         the 'unspecified context' case
     */
    public static Resource[] rewriteAndFilter(final Resource[] contexts,
            final Resource nullContextReplacement,
            final Predicate<? super Resource> removePredicate,
            @Nullable final String warningMessageOnRemove)
    {
        if (contexts.length == 0) {
            return Contexts.UNSPECIFIED;
        }

        Resource[] filteredContexts = contexts;

        // Rewrite and filtering loop. Context array cloned only if must be modified. Variable
        // length stores the length of the resulting array.
        int length = 0;
        boolean cloned = false;
        for (int i = 0; i < filteredContexts.length; ++i) {
            final boolean isNullContext = filteredContexts[i] == null;
            if (!isNullContext && !removePredicate.apply(filteredContexts[i])) {
                if (length != i) {
                    filteredContexts[length] = filteredContexts[i];
                }
                ++length;
            } else {
                if (!cloned) {
                    filteredContexts = filteredContexts.clone();
                    cloned = true;
                }
                if (isNullContext) {
                    filteredContexts[length++] = nullContextReplacement;
                }
            }
        }

        if (length < contexts.length && warningMessageOnRemove != null) {
            LOGGER.warn(warningMessageOnRemove, contexts.length - length);
        }

        if (length == 0) {
            return Contexts.NONE;
        }

        if (length < filteredContexts.length) {
            final Resource[] oldContexts = filteredContexts;
            filteredContexts = new Resource[length];
            System.arraycopy(oldContexts, 0, filteredContexts, 0, length);
        }
        return filteredContexts;
    }

    /**
     * Rewrites and filters the supplied statements <tt>Iterable</tt>, moving statements from the
     * null context to a replacement context and removing statements in contexts satisfying the
     * supplied predicate. The method wraps the supplied iterable, so to filter and rewrite
     * statements at iteration time.
     * 
     * @param iterable
     *            the statements to process
     * @param valueFactory
     *            the factory to be used to create statements in the context replacing the null
     *            one
     * @param nullContextReplacement
     *            the context to be used in place of the null context
     * @param removePredicate
     *            the predicate to be satisfied by contexts of removed statements
     * @param warningMessageOnRemove
     *            an optional warning message logged at <tt>WARN</tt> level in case statemnet
     *            removal occurs; this parameter is actually a template with an optional
     *            placeholder ( <tt>{}</tt>) which is replaced by the number of statements removed
     * @return a statement <tt>Iterable</tt> that performs rewriting and filtering at iteration
     *         time
     */
    public static Iterable<Statement> rewriteAndFilter(
            final Iterable<? extends Statement> iterable, final ValueFactory valueFactory,
            final Resource nullContextReplacement,
            final Predicate<? super Resource> removePredicate,
            @Nullable final String warningMessageOnRemove)
    {
        return new Iterable<Statement>() {

            @Override
            public Iterator<Statement> iterator()
            {
                return new AbstractIterator<Statement>() {

                    private final Iterator<? extends Statement> iterator = iterable.iterator();

                    private int skipped = 0;

                    @Override
                    protected Statement computeNext()
                    {
                        while (this.iterator.hasNext()) {

                            Statement statement = this.iterator.next();
                            final Resource context = statement.getContext();

                            if (context == null) {
                                statement = valueFactory.createStatement(statement.getSubject(),
                                        statement.getPredicate(), statement.getObject(),
                                        nullContextReplacement);

                            } else if (removePredicate.apply(context)) {
                                ++this.skipped;
                                continue;
                            }

                            return statement;
                        }

                        // Log only at the end of the iteration to reduce verbosity.
                        if (this.skipped != 0 && warningMessageOnRemove != null) {
                            LOGGER.warn(warningMessageOnRemove, this.skipped);
                        }
                        return endOfData();
                    }

                };
            }

        };
    }

    /**
     * Prevents class instantiation.
     */
    private Contexts()
    {
    }

}
