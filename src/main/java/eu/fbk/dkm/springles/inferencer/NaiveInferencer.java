package eu.fbk.dkm.springles.inferencer;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;

import org.eclipse.rdf4j.common.iteration.Iterations;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.impl.ListBindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.dkm.internal.util.Algebra;
import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.dkm.springles.inferencer.Buffer.Appender;
import eu.fbk.dkm.springles.ruleset.ClosureEvalTask;
import eu.fbk.dkm.springles.ruleset.ClosureFixPointTask;
import eu.fbk.dkm.springles.ruleset.ClosureRepeatTask;
import eu.fbk.dkm.springles.ruleset.ClosureSequenceTask;
import eu.fbk.dkm.springles.ruleset.ClosureTask;
import eu.fbk.dkm.springles.ruleset.Rule;
import eu.fbk.dkm.springles.ruleset.Ruleset;

// TODO: binding-aware rule activation/deactivation
// TODO: test with scalable repository
// TODO: semi-naive mode

class NaiveInferencer extends AbstractInferencer
{

    private static final Logger LOGGER = LoggerFactory.getLogger(NaiveInferencer.class);

    private static final int INITIAL_BUFFER_CAPACITY = 1024;

    private final Ruleset ruleset;

    private BindingSet rulesetBindings;

    private final int maxConcurrentRules;

    private final Map<Resource, RuleStatistics> statistics;

    int cont = 0;

    public NaiveInferencer(final Ruleset ruleset, @Nullable final BindingSet rulesetBindings,
            final int maxConcurrentRules)
    {
        ruleset.validate();

        int concurrencyLevel = maxConcurrentRules;
        if (concurrencyLevel <= 0) {
            concurrencyLevel = Runtime.getRuntime().availableProcessors();
        }

        this.ruleset = ruleset.isFrozen() ? ruleset : ruleset.clone();
        NaiveInferencer.LOGGER.info("Costruttore Naive Inferencer Bindings1=[ {} ]",
                rulesetBindings);
        this.rulesetBindings = ruleset.getParameterBindings(rulesetBindings);
        NaiveInferencer.LOGGER.info("Costruttore Naive Inferencer Bindings2=[ {} ]",
                this.rulesetBindings);

        this.maxConcurrentRules = concurrencyLevel;
        this.statistics = Maps.newHashMap();

        this.ruleset.freeze();

        for (final Rule rule : ruleset.getRules()) {
            this.statistics.put(rule.getID(), new RuleStatistics(rule.getID()));
        }
    }

    @Override
    protected InferenceMode doInitialize(final String inferredContextPrefix, final Hasher hasher)
            throws Exception
    {
        NaiveInferencer.LOGGER.info("inizializeinferencerbindings={}", this.rulesetBindings);
        NaiveInferencer.LOGGER.info("inferredContextPrefix={}", this.rulesetBindings);
        this.rulesetBindings = NaiveInferencer.overrideBindings(this.rulesetBindings,
                new ListBindingSet(ImmutableList.of("prefix"),
                        SimpleValueFactory.getInstance().createLiteral(inferredContextPrefix)));

        NaiveInferencer.LOGGER.info("inizializeDinferencerbindings={}", this.rulesetBindings);
        hasher.putUnencodedChars(this.ruleset.digest());
        for (final String name : Ordering.natural()
                .sortedCopy(this.rulesetBindings.getBindingNames())) {
            hasher.putUnencodedChars(name)
                    .putUnencodedChars(this.rulesetBindings.getValue(name).stringValue());
        }

        return InferenceMode.FORWARD;
    }

    @Override
    public Session newSession(final String id, final ClosureStatus closureStatus,
            final Context context) throws RepositoryException
    {
        return new NaiveSession(id, context);
    }

    @Override
    public void close() throws RepositoryException
    {
        if (NaiveInferencer.LOGGER.isInfoEnabled()) {
            final StringBuilder builder = new StringBuilder("Inference statistics:");
            for (final Rule rule : this.ruleset.getRules()) {
                builder.append("\n  ").append(this.statistics.get(rule.getID()).toString());
            }
            NaiveInferencer.LOGGER.info(builder.toString());
            NaiveInferencer.LOGGER.info("Inference buffer statistics: " + Buffer.getStatistics());
        }
    }

    protected static final BindingSet overrideBindings(final BindingSet baseBindings,
            final Map<String, ValueExpr> overridingExpressions) throws QueryEvaluationException
    {
        final MapBindingSet newBindings = new MapBindingSet();

        for (final String name : baseBindings.getBindingNames()) {
            newBindings.addBinding(name, baseBindings.getValue(name));
        }

        for (final Map.Entry<String, ValueExpr> entry : overridingExpressions.entrySet()) {
            final String name = entry.getKey();
            final Value value = Algebra.evaluateValueExpr(entry.getValue(), baseBindings,
                    SimpleValueFactory.getInstance());
            newBindings.addBinding(name, value);
        }

        return newBindings;
    }

    protected static final BindingSet overrideBindings(final BindingSet baseBindings,
            final BindingSet overridingBindings)
    {
        if (baseBindings.size() == 0) {
            return overridingBindings;
        } else if (overridingBindings.size() == 0) {
            return baseBindings;
        } else {
            final MapBindingSet newBindings = new MapBindingSet();
            for (final String name : baseBindings.getBindingNames()) {
                newBindings.addBinding(name, baseBindings.getValue(name));
            }
            for (final String name : overridingBindings.getBindingNames()) {
                newBindings.addBinding(name, overridingBindings.getValue(name));
            }
            return newBindings;
        }
    }

    protected class NaiveSession extends AbstractSession
    {

        private final String id;

        private final Context context;

        private List<Statement> buffer;

        private final Set<Resource> activeRules;

        private BindingSet lastBindings;

        public NaiveSession(final String id, final Context context)
        {
            this.id = id;
            this.context = context;
            this.buffer = null;
            this.activeRules = Sets.newHashSet();
            this.lastBindings = null;
        }

        @Override
        public final void updateClosure(final ClosureStatus closureStatus)
                throws RepositoryException
        {
            switch (closureStatus) {
            case CURRENT:
                return;

            case STALE:
                this.context.removeInferred(null, null, null, new Resource[] {});
                // return;
            case POSSIBLY_INCOMPLETE:
                try {
                    NaiveInferencer.LOGGER.info("[{}] === Closure computation started ===",
                            this.id);

                    long time = System.currentTimeMillis();
                    this.buffer = Lists
                            .newArrayListWithCapacity(NaiveInferencer.INITIAL_BUFFER_CAPACITY);
                    this.activeRules.addAll(NaiveInferencer.this.ruleset.getForwardRuleIDs());
                    for (final Resource r : this.activeRules) {
                        NaiveInferencer.LOGGER.info("{}", r.stringValue());
                    }
                    this.lastBindings = null;

                    NaiveInferencer.LOGGER.info("NaiveInferencerClosurePlan ={}  Bindings ={}",
                            NaiveInferencer.this.ruleset.getClosurePlan(),
                            NaiveInferencer.this.rulesetBindings);
                    final long inferred = this.executeTask(
                            NaiveInferencer.this.ruleset.getClosurePlan(),
                            NaiveInferencer.this.rulesetBindings);
                    this.buffer = null;
                    time = System.currentTimeMillis() - time;

                    if (NaiveInferencer.LOGGER.isDebugEnabled()) {
                        NaiveInferencer.LOGGER.debug(
                                "[{}] === Closure computation completed after {} ms with "
                                        + "{} new inferences ===",
                                new Object[] { this.id, time, inferred });
                    }

                } catch (final QueryEvaluationException ex) {
                    throw new RepositoryException("Closure computation failed: " + ex.getMessage(),
                            ex);
                }
                break;

            default:
                throw new Error("Unknown closure status: " + closureStatus);
            }
        }

        protected final List<Statement> getBuffer()
        {
            return this.buffer;
        }

        protected final long executeTask(final ClosureTask task, final BindingSet bindings)
                throws QueryEvaluationException, RepositoryException
        {
            NaiveInferencer.LOGGER.info("[Execute Tsk] Bindings {} - {}", bindings,
                    NaiveInferencer.this.cont);
            BindingSet actualBindings = bindings;
            if (!task.getBindings().isEmpty()) {
                actualBindings = NaiveInferencer.overrideBindings(bindings, task.getBindings());
                NaiveInferencer.LOGGER.info("[Execute Tsk] ActualBindings {} - {}", actualBindings,
                        NaiveInferencer.this.cont);
            }

            if (!(task instanceof ClosureEvalTask)) {
                NaiveInferencer.LOGGER.info("[{}] --- Executing {} ---", this.id, task);
            }
            NaiveInferencer.LOGGER.info("{} - {}", actualBindings.getBindingNames().toString(),
                    NaiveInferencer.this.cont);
            long result;
            if (task instanceof ClosureSequenceTask) {
                NaiveInferencer.LOGGER.info("[{}] --- Executing {} ---", this.id, task);
                result = this.executeSequence((ClosureSequenceTask) task, actualBindings);
            } else if (task instanceof ClosureFixPointTask) {
                NaiveInferencer.LOGGER.info("[{}] --- Executing {} ---", this.id, task);
                result = this.executeFixPoint((ClosureFixPointTask) task, actualBindings);
            } else if (task instanceof ClosureRepeatTask) {
                NaiveInferencer.LOGGER.info("[{}] --- Executing {} ---", this.id, task);
                result = this.executeRepeat((ClosureRepeatTask) task, actualBindings);
            } else if (task instanceof ClosureEvalTask) {
                NaiveInferencer.LOGGER.info("[{}] --- Executing {} ---", this.id, task);
                NaiveInferencer.LOGGER.info("EXECUTE EVAL TASK --- ActualBindings {} ---", this.id,
                        actualBindings);
                result = this.executeEval((ClosureEvalTask) task, actualBindings);
            } else {
                throw new Error("Unknown closure task: " + task.getClass().getSimpleName());
            }

            return result;
        }

        protected long executeSequence(final ClosureSequenceTask task, final BindingSet bindings)
                throws QueryEvaluationException, RepositoryException
        {
            long result = 0L;
            for (final ClosureTask subTask : task.getSubTasks()) {
                final long inferred = this.executeTask(subTask, bindings);
                result += inferred;
            }
            return result;
        }

        protected long executeFixPoint(final ClosureFixPointTask task, final BindingSet bindings)
                throws QueryEvaluationException, RepositoryException
        {
            long result = 0L;
            // int iteration = 1;
            while (true) {
                // LOGGER.info("[{}] Fixxxxx point iteration {} started", this.id, iteration);
                // LOGGER.info("[{}] Fixxxxx point bindings {} started", this.id, bindings);
                // #TODO GC
                final long size_before = this.context.size(true);
                this.executeTask(task.getSubTask(), bindings);
                final long size_after = this.context.size(true);

                final long inferred = size_after - size_before;

                // if (LOGGER.isInfoEnabled()) {
                // LOGGER.info("[{}] Fixxxxx point iteration {} completed with {} new inferences",
                // new Object[] { this.id, iteration, inferred });
                // }
                // LOGGER.info("{}",inferred);
                result += inferred;
                // ++iteration;
                ++NaiveInferencer.this.cont;
                // if (inferred == 0L || cont > 150) {
                if (inferred == 0L) {

                    break;
                }
            }

            return result;
        }

        protected long executeRepeat(final ClosureRepeatTask task, final BindingSet bindings)
                throws QueryEvaluationException, RepositoryException
        {
            final List<BindingSet> iterationRange;
            try {
                iterationRange = Iterations
                        .asList(this.context.query(task.getQuery(), null, bindings, true, 0));
            } catch (final MalformedQueryException ex) {
                throw new Error("Unexpected exception: " + ex.getMessage(), ex);
            }

            long result = 0L;
            int iteration = 1;

            for (final BindingSet iterationBindings : iterationRange) {

                if (NaiveInferencer.LOGGER.isDebugEnabled()) {
                    NaiveInferencer.LOGGER.debug("[{}] Repeat iteration {}/{} started",
                            new Object[] { this.id, iteration, iterationRange.size() });
                }

                final BindingSet actualBindings = NaiveInferencer.overrideBindings(bindings,
                        iterationBindings);
                final long inferred = this.executeTask(task.getSubTask(), actualBindings);

                if (NaiveInferencer.LOGGER.isDebugEnabled()) {
                    NaiveInferencer.LOGGER.debug(
                            "[{}] Repeat iteration {}/{} completed with {} new inferences",
                            new Object[] { this.id, iteration, iterationRange.size(), inferred });
                }

                result += inferred;
                ++iteration;
            }

            return result;
        }

        protected long executeEval(final ClosureEvalTask task, final BindingSet bindings)
                throws QueryEvaluationException, RepositoryException
        {
            NaiveInferencer.LOGGER.info("executeEval [ {} ]", task);

            // LOGGER.info("GAETANO++++Bindings [ {} ]
            // --LansBIndings[{}]+++",bindings,this.lastBindings);

            if (!bindings.equals(this.lastBindings)) {
                this.activeRules.addAll(NaiveInferencer.this.ruleset.getForwardRuleIDs());
                this.lastBindings = bindings;
            }

            final Buffer buffer = new Buffer(this.context.getValueFactory());

            final Queue<Resource> pendingRuleIDs = new ArrayDeque<Resource>();
            for (final Resource ruleID : task.getRuleIDs()) {
                final Rule rule = NaiveInferencer.this.ruleset.getRule(ruleID);
                if (!this.activeRules.contains(ruleID)) {
                    NaiveInferencer.LOGGER.debug("[{}] Rule {} skipped because inactive", this.id,
                            ruleID);
                } else if (rule.getCondition() != null
                        && !((Literal) Algebra.evaluateValueExpr(rule.getCondition(), bindings,
                                SimpleValueFactory.getInstance())).booleanValue()) {
                    NaiveInferencer.LOGGER.debug(
                            "[{}] Rule {} skipped because condition unsatisfied", this.id, ruleID);
                } else {

                    // for (final Resource resource : this.activeRules) {
                    // System.out.println(resource.toString());
                    // }

                    this.activeRules.remove(ruleID);
                    pendingRuleIDs.offer(ruleID);
                }
            }

            if (pendingRuleIDs.isEmpty()) {
                return 0L;
            }

            final int numAuxiliaryTasks = this.context.getScheduler() == null ? 0
                    : Math.min(NaiveInferencer.this.maxConcurrentRules, pendingRuleIDs.size()) - 1;
            final List<Future<?>> futures = Lists.newArrayListWithCapacity(numAuxiliaryTasks);
            /*
             * for (int i = 0; i < numAuxiliaryTasks; ++i) {
             * futures.add(this.context.getScheduler().submit(new Callable<Void>() {
             * 
             * @Override public Void call() throws Exception { executeEvalHelper(pendingRuleIDs,
             * bindings, buffer); return null; }
             * 
             * })); }
             */

            this.executeEvalHelper(pendingRuleIDs, bindings, buffer);

            for (final Future<?> future : futures) {
                try {
                    future.get();
                } catch (final ExecutionException ex) {
                    throw new RepositoryException(
                            "Rule evaluation failed: " + ex.getCause().getMessage(),
                            ex.getCause());
                } catch (final InterruptedException ex) {
                    throw new RepositoryException("Rule evaluation interrupted", ex);
                }
            }

            if (buffer.size() > 0) {
                NaiveInferencer.LOGGER.info("[{}] Flushing {} inferred statements to repository",
                        this.id, buffer.size());
                this.context.addInferred(buffer);
            }

            return buffer.size();
        }

        private void executeEvalHelper(final Queue<Resource> pendingRuleIDs,
                final BindingSet bindings, final Buffer buffer)
                throws QueryEvaluationException, RepositoryException
        {
            while (true) {
                Rule rule = null;
                synchronized (this) {
                    final Resource ruleID = pendingRuleIDs.poll();
                    if (ruleID == null) {
                        return;
                    }
                    rule = NaiveInferencer.this.ruleset.getRule(ruleID);
                }

                long time = System.currentTimeMillis();
                final int count = this.evaluateRule(rule, bindings, buffer);
                time = System.currentTimeMillis() - time;

                synchronized (this) {
                    NaiveInferencer.this.statistics.get(rule.getID()).recordActivations(count,
                            time);
                    if (count > 0) {
                        this.activeRules
                                .addAll(MoreObjects.firstNonNull(rule.getTriggeredRuleIDs(),
                                        NaiveInferencer.this.ruleset.getForwardRuleIDs()));
                    }
                }

                if (NaiveInferencer.LOGGER.isInfoEnabled()) {
                    NaiveInferencer.LOGGER.info(
                            "[{}] Rule {} evaluated in {} ms, {} statements inferred",
                            new Object[] { this.id, rule, time, count });
                }
            }
        }

        protected int evaluateRule(final Rule rule, final BindingSet bindings, final Buffer buffer)
                throws QueryEvaluationException, RepositoryException
        {
            try {
                final TupleQueryResult iteration = this.context.query(rule.getBodyQuery(), null,
                        bindings, true, 0);
                try {
                    final Appender appender = buffer.newAppender();
                    rule.collectHeadStatements(iteration, bindings, appender);
                    return appender.flush();
                } finally {
                    iteration.close();
                }

            } catch (final MalformedQueryException ex) {
                throw new Error("Unexpected exception: " + ex.getMessage(), ex);
            }
        }

    }

    private final static class RuleStatistics
    {

        private final Resource ruleID;

        private int activations;

        private long statements;

        private long time;

        public RuleStatistics(final Resource ruleID)
        {
            this.ruleID = ruleID;
            this.activations = 0;
            this.statements = 0L;
            this.time = 0L;
        }

        public void recordActivations(final long statements, final long time)
        {
            ++this.activations;
            this.statements += statements;
            this.time += time;
        }

        @Override
        public String toString()
        {
            final String id = this.ruleID instanceof IRI ? ((IRI) this.ruleID).getLocalName()
                    : this.ruleID.stringValue();
            return String.format("%-20s %6d activation(s) %8d inf. statement(s) %8d ms total", id,
                    this.activations, this.statements, this.time);
        }

    }

}
