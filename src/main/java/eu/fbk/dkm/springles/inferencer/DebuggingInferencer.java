package eu.fbk.dkm.springles.inferencer;

import java.util.Arrays;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;

import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.dkm.springles.base.QuerySpec;
import eu.fbk.dkm.springles.base.UpdateSpec;

/**
 * An <tt>Inferencer</tt> decorator that logs method calls and checks they respect the
 * <tt>Inferencer</tt> API and lifecycle.
 * <p>
 * This decorator is used for debugging purposes. It logs method invocations (at <tt>DEBUG</tt>
 * level) using a supplied {@link Logger} and checks preconditions concerning both method
 * parameters and invocation order w.r.t. the prescribed <tt>Inferencer</tt> lifecycle.
 */
final class DebuggingInferencer extends ForwardingInferencer
{

    /** The wrapped inferencer. */
    private final Inferencer delegate;

    /** The logger instance to be used. */
    private final Logger logger;

    /** Flag being <tt>true</tt> if the inferencer has been initialized. */
    private volatile boolean initialized;

    /** Flag being <tt>true</tt> if the inferencer has been closed. */
    private volatile boolean closed;

    /**
     * Creates a new wrapper instance using the wrapped inference and logger supplied
     * 
     * @param delegate
     *            the wrapped inferencer
     * @param logger
     *            the logger used to log method invocations
     */
    public DebuggingInferencer(final Inferencer delegate, final Logger logger)
    {
        Preconditions.checkNotNull(delegate);
        Preconditions.checkNotNull(logger);

        this.delegate = delegate;
        this.logger = logger;
        this.initialized = false;
        this.closed = false;
    }

    /**
     * {@inheritDoc} Returns the wrapped inferencer set at construction time.
     */
    @Override
    protected Inferencer delegate()
    {
        return this.delegate;
    }

    /**
     * {@inheritDoc} Delegates and logs, checking the invocation respects the inferencer
     * lifecycle.
     */
    @Override
    public void initialize(final String inferredContextPrefix) throws RepositoryException
    {
        Preconditions.checkNotNull(inferredContextPrefix);
        Preconditions.checkState(!this.closed && !this.initialized);

        this.delegate.initialize(inferredContextPrefix);
        this.initialized = true;

        if (this.logger.isDebugEnabled()) {
            this.logger.debug("Inferencer initialized: inference mode = " + getInferenceMode()
                    + ", configuration digest = " + getConfigurationDigest());
        }
    }

    /**
     * {@inheritDoc} Delegates, checking the invocation respects the inferencer lifecycle.
     */
    @Override
    public InferenceMode getInferenceMode()
    {
        Preconditions.checkState(this.initialized && !this.closed);
        return this.delegate.getInferenceMode();
    }

    /**
     * {@inheritDoc} Delegates, checking the invocation respects the inferencer lifecycle.
     */
    @Override
    public String getConfigurationDigest()
    {
        Preconditions.checkState(this.initialized && !this.closed);
        return this.delegate.getConfigurationDigest();
    }

    /**
     * {@inheritDoc} Delegates and logs, wrapping the returned <tt>Session</tt> so to log and
     * check method calls and checking that the method invocation respects the inferencer
     * lifecycle.
     */
    @Override
    public Session newSession(final String id, final ClosureStatus closureStatus,
            final Context context) throws RepositoryException
    {
        Preconditions.checkNotNull(closureStatus);
        Preconditions.checkNotNull(context);
        Preconditions.checkState(this.initialized && !this.closed);

        final Session session = this.delegate.newSession(id, closureStatus, context);
        Preconditions.checkNotNull(session);
        this.logger.debug("[{}] Inference session created, closure status is {}", id,
                closureStatus);

        return new DebuggingSession(session, this.logger, id);
    }

    /**
     * {@inheritDoc} Delegates and logs, checking the invocation respects the inferencer
     * lifecycle.
     */
    @Override
    public void close() throws RepositoryException
    {
        Preconditions.checkState(!this.closed && this.initialized);
        try {
            this.delegate.close();
            this.logger.debug("Inferencer closed");
        } finally {
            this.closed = true;
        }
    };

    /**
     * An <tt>Inferencer.Session</tt> decorator that logs method calls and checks they respect the
     * Inferencer API and lifecycle.
     * <p>
     * This class is used together with {@link DebuggingInferencer}.
     * </p>
     */
    final class DebuggingSession implements Session
    {

        /** The wrapped inferencer session. */
        private final Session delegate;

        /** The logger used to log method invocations (at debug level). */
        private final Logger logger;

        /** The inferencer session ID. */
        private final String id;

        /** Flag being <tt>true</tt> if the session has been closed. */
        private volatile boolean closed;

        /**
         * Creates a new wrapper instance, using the wrapped inference session and logger
         * supplied.
         * 
         * @param delegate
         *            the wrapped inferencer session
         * @param logger
         *            the logger for logging method invocations
         * @param id
         *            the session ID
         */
        public DebuggingSession(final Session delegate, final Logger logger, final String id)
        {
            this.delegate = delegate;
            this.logger = logger;
            this.id = id;
            this.closed = false;
        }

        /**
         * Helper method for logging invocations of
         * {@link #statementsAdded(Iterable, Resource...)} and
         * {@link #statementsRemoved(Iterable, Resource...)}.
         * 
         * @param operation
         *            the operation being put in the log message, e.g., <tt>addition</tt> or
         *            <tt>removal</tt>
         * @param statements
         *            the <tt>statements</tt> argument of the notification
         * @param contexts
         *            the <tt>contexts</tt> argument of the notification
         */
        private void logNotification(final String operation,
                final Iterable<? extends Statement> statements, final Resource[] contexts)
        {
            final String sizeString = statements == null ? "an unprecised number of" : ""
                    + Iterables.size(statements);
            final String contextsString = contexts.length == 0 ? "" : " in contexts "
                    + Arrays.toString(contexts);
            this.logger.debug("[{}] Inference session notified of {} of {} statements{}",
                    new Object[] { this.id, operation, sizeString, contextsString });
        }

        /**
         * Helper method for logging invocations of {@link #rewriteQuery(QuerySpec, boolean)} and
         * {@link #rewriteUpdate(UpdateSpec, boolean)}.
         * 
         * @param expression
         *            the {@link QuerySpec} or {@link UpdateSpec} being rewritten
         * @param closureStatus
         *            the current <tt>ClosureStatus</tt> for the repository
         * @param forwardInferenceEnabled
         *            the <tt>forwardInferenceEnabled</tt> argument of the rewrite request
         */
        private void logRewrite(final Object expression, final ClosureStatus closureStatus,
                final boolean forwardInferenceEnabled)
        {
            this.logger.debug("[{}] Rewriting {} with forward inference {} "
                    + "and current closure status {}:\n{}", new Object[] { this.id,
                    expression instanceof QuerySpec<?> ? "query" : "update",
                    forwardInferenceEnabled ? "enabled" : "disabled", closureStatus, expression });
        }

        /**
         * {@inheritDoc} Delegates and logs, checking parameters and closed status.
         */
        @Override
        public void statementsAdded(@Nullable final Iterable<? extends Statement> statements,
                final Resource... contexts) throws RepositoryException
        {
            Preconditions.checkNotNull(contexts);
            Preconditions.checkState(!this.closed);

            if (this.logger.isDebugEnabled()) {
                logNotification("addition", statements, contexts);
            }

            this.delegate.statementsAdded(statements, contexts);
        }

        /**
         * {@inheritDoc} Delegates and logs, checking parameters and closed status.
         */
        @Override
        public void statementsRemoved(@Nullable final Iterable<? extends Statement> statements,
                final Resource... contexts) throws RepositoryException
        {
            Preconditions.checkNotNull(contexts);
            Preconditions.checkState(!this.closed);

            if (this.logger.isDebugEnabled()) {
                logNotification("removal", statements, contexts);
            }

            this.delegate.statementsRemoved(statements, contexts);
        }

        /**
         * {@inheritDoc} Delegates and logs, checking parameters and closed status.
         */
        @Override
        public void statementsCleared(final boolean onlyClosure) throws RepositoryException
        {
            Preconditions.checkState(!this.closed);

            if (this.logger.isDebugEnabled()) {
                this.logger.debug("[{}] Notifying inference session of statements cleared, "
                        + "{} closure", this.id, onlyClosure ? "excluding" : "including");
            }

            this.delegate.statementsCleared(onlyClosure);
        }

        /**
         * {@inheritDoc} Delegates and logs, checking parameters and closed status.
         */
        @Override
        public <T> QuerySpec<T> rewriteQuery(final QuerySpec<T> query,
                final ClosureStatus closureStatus, final boolean forwardInferenceEnabled)
                throws RepositoryException
        {
            Preconditions.checkNotNull(query);
            Preconditions.checkNotNull(closureStatus);
            Preconditions.checkState(!this.closed);

            if (this.logger.isDebugEnabled()) {
                logRewrite(query, closureStatus, forwardInferenceEnabled);
            }

            return this.delegate.rewriteQuery(query, closureStatus, forwardInferenceEnabled);
        }

        /**
         * {@inheritDoc} Delegates and logs, checking parameters and closed status.
         */
        @Override
        public UpdateSpec rewriteUpdate(final UpdateSpec update,
                final ClosureStatus closureStatus, final boolean forwardInferenceEnabled)
                throws RepositoryException
        {
            Preconditions.checkNotNull(update);
            Preconditions.checkNotNull(closureStatus);
            Preconditions.checkState(!this.closed);

            if (this.logger.isDebugEnabled()) {
                logRewrite(update, closureStatus, forwardInferenceEnabled);
            }

            return this.delegate.rewriteUpdate(update, closureStatus, forwardInferenceEnabled);
        }

        /**
         * {@inheritDoc} Delegates and logs, checking parameters and closed status.
         */
        @Override
        public void updateClosure(final ClosureStatus closureStatus) throws RepositoryException
        {
            Preconditions.checkState(!this.closed);

            this.logger.debug("[{}] Updating closure, closure status is {}", this.id,
                    closureStatus);

            this.delegate.updateClosure(closureStatus);
        }

        /**
         * {@inheritDoc} Delegates and logs, checking parameters and closed status.
         */
        @Override
        public void close(final boolean committing) throws RepositoryException
        {
            Preconditions.checkState(!this.closed);

            try {
                this.delegate.close(committing);
                this.logger.debug("[{}] Inference session closed", this.id);
            } finally {
                this.closed = true;
            }
        }

    }

}
