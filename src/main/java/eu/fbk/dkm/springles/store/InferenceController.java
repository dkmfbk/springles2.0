package eu.fbk.dkm.springles.store;

import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.dkm.internal.util.URIPrefix;
import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.base.QuerySpec;
import eu.fbk.dkm.springles.base.Transaction;
import eu.fbk.dkm.springles.base.UpdateSpec;
import eu.fbk.dkm.springles.inferencer.Inferencer;
import eu.fbk.dkm.springles.inferencer.Inferencer.Session;

/**
 * Utility class mediating the access to {@link InferencerSession}.
 * <p>
 * This class helps in properly accessing and managing an {@link InferencerSession}. The creation
 * and the call of any of the session methods are performed through an instance of this class,
 * which takes care of (1) properly managing the session {@link InferencerContext} (locking and
 * unlocking); (2) wrapping {@link InferenceException}s; (3) checking call parameters and prevent
 * reentrancy and (4) suppress notifications to the {@link InferencerSession} that originates from
 * write operations performed by the session itself.
 * </p>
 */
final class InferenceController
{

    private static final Logger LOGGER = LoggerFactory.getLogger(InferenceController.class);

    private final Session session;

    private final InferenceContext context;

    public InferenceController(final Inferencer inferencer, final Transaction transaction,
            final URIPrefix inferredContextPrefix,
            @Nullable final ScheduledExecutorService scheduler, final ClosureStatus closureStatus)
            throws RepositoryException
    {
        Preconditions.checkNotNull(inferencer);
        Preconditions.checkNotNull(transaction);
        Preconditions.checkNotNull(inferredContextPrefix);
        Preconditions.checkNotNull(closureStatus);

        this.context = new InferenceContext(transaction, inferredContextPrefix, scheduler);
        InferenceController.LOGGER.info("INFERENCER::" + inferencer.toString());
        this.context.unlock(false);
        try {
            this.session = inferencer.newSession(transaction.getID(), closureStatus, this.context);

        } catch (final Exception ex) {
            throw new RepositoryException("Inference session initialization failed", ex);

        } finally {
            this.context.lock();
        }
    }

    public synchronized void statementsAdded(
            @Nullable final Iterable<? extends Statement> statements, final Resource... contexts)
    {
        Preconditions.checkNotNull(contexts);

        if (this.isNotificationEnabled()) {
            this.context.unlock(false);
            try {
                this.session.statementsAdded(statements, contexts);

            } catch (final Throwable ex) {
                InferenceController.LOGGER
                        .warn("Exception thrown by InferenceSession.statementsAdded() "
                                + "callback: ignoring", ex);
            }
            this.context.lock();
        }
    }

    public synchronized void statementsRemoved(
            @Nullable final Iterable<? extends Statement> statements, final Resource... contexts)
    {
        Preconditions.checkNotNull(contexts);

        if (this.isNotificationEnabled()) {
            this.context.unlock(false);
            try {
                this.session.statementsRemoved(statements, contexts);

            } catch (final Throwable ex) {
                InferenceController.LOGGER
                        .warn("Exception thrown by InferenceSession.statementsRemoved() "
                                + "callback: ignoring", ex);
            }
            this.context.lock();
        }
    }

    public synchronized void statementsCleared(final boolean onlyClosure)
    {
        if (this.isNotificationEnabled()) {
            this.context.unlock(false);
            try {
                this.session.statementsCleared(onlyClosure);

            } catch (final Throwable ex) {
                InferenceController.LOGGER
                        .warn("Exception thrown by InferenceSession.statementsCleared() "
                                + "callback: ignoring", ex);
            }
            this.context.lock();
        }
    }

    public synchronized <T> QuerySpec<T> rewriteQuery(final QuerySpec<T> query,
            final ClosureStatus closureStatus, final boolean forwardInferenceEnabled)
            throws RepositoryException
    {
        this.checkInferenceSessionInactive();
        Preconditions.checkNotNull(query);

        this.context.unlock(false);
        try {
            final QuerySpec<T> rewrittenQuery = this.session.rewriteQuery(query, closureStatus,
                    forwardInferenceEnabled);

            // Additional check to protect from ill-behaving inference engines.
            if (rewrittenQuery != null && rewrittenQuery.getType() != query.getType()) {
                throw new Error("Rewritten query has different type from input query. Input:\n"
                        + query + "\noutput:\n" + rewrittenQuery);
            }

            return rewrittenQuery;

        } catch (final Exception ex) {
            throw new RepositoryException("Query rewriting failed. Input:\n" + query, ex);

        } finally {
            this.context.lock();
        }
    }

    public synchronized UpdateSpec rewriteUpdate(final UpdateSpec update,
            final ClosureStatus closureStatus, final boolean forwardInferenceEnabled)
            throws RepositoryException
    {
        this.checkInferenceSessionInactive();
        Preconditions.checkNotNull(update);

        this.context.unlock(false);
        try {
            return this.session.rewriteUpdate(update, closureStatus, forwardInferenceEnabled);

        } catch (final Exception ex) {
            throw new RepositoryException("Update rewriting failed for input:\n" + update, ex);

        } finally {
            this.context.lock();
        }
    }

    public synchronized void updateClosure(final ClosureStatus closureStatus)
            throws RepositoryException
    {
        this.checkInferenceSessionInactive();

        this.context.unlock(true);
        try {
            this.session.updateClosure(closureStatus);

        } catch (final Exception ex) {
            throw new RepositoryException("Closure update failed: " + ex.getMessage(), ex);

        } finally {
            this.context.lock();
        }
    }

    public synchronized void close(final boolean committing) throws RepositoryException
    {
        this.checkInferenceSessionInactive();

        this.context.unlock(committing);
        try {
            this.session.close(committing);

        } catch (final Exception ex) {
            if (committing) {
                throw new RepositoryException(
                        "Cannot finalize inference session during commit: commit will fail", ex);
            } else {
                InferenceController.LOGGER
                        .error("Exception thrown by InferenceSession.close(committing = false): "
                                + "ignoring", ex);
            }

        } finally {
            this.context.lock();
        }
    }

    private boolean isNotificationEnabled()
    {
        // If unlocked, then the operation to notify originates from a call to the inference
        // engine, and thus should not be notified.
        return !this.context.isUnlocked();
    }

    private void checkInferenceSessionInactive()
    {
        if (this.context.isUnlocked()) {
            throw new Error("Only an inference session operation can be active at a time (!)");
        }
    }

}
