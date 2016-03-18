package eu.fbk.dkm.springles.store;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;

import eu.fbk.dkm.internal.util.Contexts;
import eu.fbk.dkm.internal.util.Iterations;
import eu.fbk.dkm.internal.util.URIPrefix;
import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.dkm.springles.base.QuerySpec;
import eu.fbk.dkm.springles.base.QueryType;
import eu.fbk.dkm.springles.base.Transaction;
import eu.fbk.dkm.springles.inferencer.Inferencer;
import info.aduna.iteration.CloseableIteration;

/**
 * Implementation of {@link InferencerContext} on top of a {@link Transaction}.
 * <p>
 * This class implements the {@link InferencerContext} interface used by {@link InferencerSession}
 * s to perform their work. This implementation is layered on top of a {@link Transaction}, with
 * context methods more or less delegated to corresponding transaction methods (note that
 * delegated methods are never asked to perform backward reasoning). Apart delegation, three main
 * features are implemented:
 * <ul>
 * <li>a locking mechanism that allows access to the context only at certain times, controlled by
 * the class user (i.e., when a method of {@link InferencerSession} is called);</li>
 * <li>a filtering mechanism that enforces the modification of closure statements only (an
 * inference engine is not supposed to modify explicit statements);</li>
 * <li>a mechanism to force closing pending iterations when the context is locked and access to it
 * prevented.</li>
 * </ul>
 * </p>
 */
class InferenceContext implements Inferencer.Context
{
	private static final Logger LOGGER = LoggerFactory.getLogger(InferenceContext.class);
	
    private final Transaction transaction;

    private final URIPrefix inferredContextPrefix;

    @Nullable
    private final ScheduledExecutorService scheduler;

    private boolean accessible;

    private boolean writable;

    private final Set<CloseableIteration<?, ?>> pendingIterations;

    public InferenceContext(final Transaction transaction, final URIPrefix inferredContextPrefix,
            @Nullable final ScheduledExecutorService scheduler)
    {
        Preconditions.checkNotNull(transaction);
        Preconditions.checkNotNull(inferredContextPrefix);

        this.transaction = transaction;
        this.inferredContextPrefix = inferredContextPrefix;
        this.scheduler = scheduler;
        this.accessible = false;
        this.writable = false;
        this.pendingIterations = Sets.newHashSet();
    }

    public Transaction getTransaction()
    {
        return this.transaction;
    }

    public URIPrefix getInferredContextPrefix()
    {
        return this.inferredContextPrefix;
    }

    public synchronized boolean isUnlocked()
    {
        return this.accessible;
    }

    public synchronized void unlock(final boolean writable)
    {
        this.accessible = true;
        this.writable = writable;
    }

    public synchronized void lock()
    {
        this.accessible = false;
        for (final CloseableIteration<?, ?> iteration : this.pendingIterations) {
            Iterations.closeQuietly(iteration);
        }
    }

    private <T, E extends Exception> CloseableIteration<T, E> register(
            final CloseableIteration<T, E> iteration)
    {
        this.pendingIterations.add(iteration);
        return iteration;
    }

    private Resource[] filter(final Resource[] contexts)
    {
        final String message = "Detected write request from inference engine affecting "
                + "{} explicit contexts. Explicit contexts removed from the operation "
                + "target. Operation will be performed on remaining contexts (if any).";
        return Contexts.filter(contexts,
                Predicates.not(this.inferredContextPrefix.valueMatcher()), message);
    }

    private Iterable<Statement> filter(final Iterable<? extends Statement> iterable)
    {
        final String message = "Detected write request from inference engine affecting "
                + "{} explicit statements. Explicit statements removed from the operation "
                + "target. Operation will be performed on remaining statements (if any).";
        return Contexts.filter(iterable,
                Predicates.not(this.inferredContextPrefix.valueMatcher()), message);
    }

    private void checkAccessible()
    {
        if (!this.accessible) {
            throw new IllegalStateException("Cannot access inference context "
                    + "outside one of the inference session method calls!");
        }
    }

    private void checkWritable()
    {
        if (!this.writable) {
            throw new IllegalStateException("Cannot write to repository through inference "
                    + "context in the scope of this inference session method call!");
        }
    }

    @Override
    @Nullable
    public ScheduledExecutorService getScheduler()
    {
        return this.scheduler;
    }

    @Override
    public synchronized ValueFactory getValueFactory()
    {
        checkAccessible();

        return this.transaction.getValueFactory();
    }

    @Override
    public synchronized <T> T query(final QuerySpec<T> query, final Dataset dataset,
            final BindingSet bindings, final boolean includeClosure, final int timeout)
            throws MalformedQueryException, QueryEvaluationException, RepositoryException
    {
        checkAccessible();

        final T result = this.transaction.query(query, dataset, bindings,
                includeClosure ? InferenceMode.FORWARD : InferenceMode.NONE, timeout);

        if (query.getType() != QueryType.BOOLEAN) {
            register((CloseableIteration<?, ?>) result);
        }

        return result;
    }

    @Override
    public synchronized CloseableIteration<? extends Resource, RepositoryException> //
    getContextIDs(final boolean includeClosure) throws RepositoryException
    {
        checkAccessible();

        return register(this.transaction.getContextIDs(includeClosure ? InferenceMode.FORWARD
                : InferenceMode.NONE));
    }

    @Override
    public synchronized CloseableIteration<? extends Statement, RepositoryException> //
    getStatements(final Resource subj, final URI pred, final Value obj,
            final boolean includeClosure, final Resource... contexts) throws RepositoryException
    {
        checkAccessible();

        return register(this.transaction.getStatements(subj, pred, obj,
                includeClosure ? InferenceMode.FORWARD : InferenceMode.NONE, contexts));
    }

    @Override
    public synchronized boolean hasStatement(final Resource subj, final URI pred, final Value obj,
            final boolean includeClosure, final Resource... contexts) throws RepositoryException
    {
        checkAccessible();

        return this.transaction.hasStatement(subj, pred, obj,
                includeClosure ? InferenceMode.FORWARD : InferenceMode.NONE, contexts);
    }

    @Override
    public synchronized long size(final boolean includeClosure, final Resource... contexts)
            throws RepositoryException
    {
        checkAccessible();

        return this.transaction.size(includeClosure ? InferenceMode.FORWARD : InferenceMode.NONE,
                contexts);
    }

    // TODO: consider failing so to avoid infinite loops

    @Override
    public synchronized void addInferred(final Iterable<? extends Statement> statements,
            final Resource... contexts) throws RepositoryException
    {
        checkAccessible();
        checkWritable();

        final Resource[] targetContexts = filter(contexts);
        if (targetContexts == Contexts.UNSPECIFIED) {

            //this.transaction.add(filter(statements), targetContexts);
        	this.transaction.add(statements, targetContexts);
        } else if (targetContexts != Contexts.NONE) {
            this.transaction.add(statements, targetContexts);
        }
    }

    @Override
    public synchronized void removeInferred(final Iterable<? extends Statement> statements,
            final Resource... contexts) throws RepositoryException
    {
        checkAccessible();
        checkWritable();

        final Resource[] targetContexts = filter(contexts);

        if (targetContexts == Contexts.UNSPECIFIED) {
            this.transaction.remove(filter(statements), targetContexts);
        } else if (targetContexts != Contexts.NONE) {
            this.transaction.remove(statements, targetContexts);
        }
    }

    @Override
    public synchronized void removeInferred(final Resource subject, final URI predicate,
            final Value object, final Resource... contexts) throws RepositoryException
    {
        checkAccessible();
        checkWritable();

        final Resource[] targetContexts = filter(contexts);

        if (targetContexts == Contexts.UNSPECIFIED) {
            final List<Resource> implicitContexts = Iterations.getAllElements(Iterations.filter(
                    this.transaction.getContextIDs(InferenceMode.FORWARD),
                    this.inferredContextPrefix.valueMatcher()));
            for (final Resource implicitContext : implicitContexts) {
                this.transaction.remove(subject, predicate, object, implicitContext);
            }

        } else if (targetContexts != Contexts.NONE) {
            this.transaction.remove(subject, predicate, object, targetContexts);
        }
    }

}