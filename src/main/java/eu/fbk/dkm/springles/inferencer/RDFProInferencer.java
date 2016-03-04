package eu.fbk.dkm.springles.inferencer;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;

import eu.fbk.dkm.internal.util.Algebra;
import eu.fbk.dkm.springles.ClosureStatus;
import eu.fbk.dkm.springles.InferenceMode;
import eu.fbk.dkm.springles.ruleset.ClosureEvalTask;
import eu.fbk.dkm.springles.ruleset.ClosureFixPointTask;
import eu.fbk.dkm.springles.ruleset.ClosureRepeatTask;
import eu.fbk.dkm.springles.ruleset.ClosureSequenceTask;
import eu.fbk.dkm.springles.ruleset.ClosureTask;
import eu.fbk.rdfpro.Rule;
import eu.fbk.rdfpro.RuleEngine;
import eu.fbk.rdfpro.Ruleset;
import info.aduna.iteration.Iterations;

// TODO: binding-aware rule activation/deactivation
// TODO: test with scalable repository
// TODO: semi-naive mode

class RDFProInferencer extends AbstractInferencer
{

    private static final Logger LOGGER = LoggerFactory.getLogger(RDFProInferencer.class);

    private static final int INITIAL_BUFFER_CAPACITY = 1024;

    private Ruleset ruleset;

    private BindingSet rulesetBindings;

    private final int maxConcurrentRules;

    private final Map<Resource, RuleStatistics> statistics;

    public RDFProInferencer(final Ruleset ruleset, @Nullable final BindingSet rulesetBindings,
            final int maxConcurrentRules)
    {
        

        int concurrencyLevel = maxConcurrentRules;
        if (concurrencyLevel <= 0) {
            concurrencyLevel = Runtime.getRuntime().availableProcessors();
        }

        this.ruleset = ruleset;
        this.rulesetBindings = rulesetBindings;
        this.maxConcurrentRules = concurrencyLevel;
        this.statistics = Maps.newHashMap();


        for (final Rule rule : ruleset.getRules()) {
            this.statistics.put(rule.getID(), new RuleStatistics(rule.getID()));
        }
    }

    @Override
    protected InferenceMode doInitialize(final String inferredContextPrefix, final Hasher hasher)
            throws Exception
    {
    	/*
    	 * this.rulesetBindings = overrideBindings(this.rulesetBindings,
                new ListBindingSet(ImmutableList.of("prefix"), ValueFactoryImpl.getInstance()
                        .createLiteral(inferredContextPrefix)));

        hasher.putUnencodedChars(this.ruleset.digest());
        for (final String name : Ordering.natural().sortedCopy(
                this.rulesetBindings.getBindingNames())) {
            hasher.putUnencodedChars(name).putUnencodedChars(this.rulesetBindings.getValue(name).stringValue());
        }
        */
        this.ruleset = ruleset.rewriteVariables(rulesetBindings);
        
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
        if (LOGGER.isInfoEnabled()) {
            final StringBuilder builder = new StringBuilder("Inference statistics:");
            for (final Rule rule : this.ruleset.getRules()) {
                builder.append("\n  ").append(this.statistics.get(rule.getID()).toString());
            }
            LOGGER.info(builder.toString());
            LOGGER.info("Inference buffer statistics: " + Buffer.getStatistics());
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
                    ValueFactoryImpl.getInstance());
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
            /*switch (closureStatus) {
            case CURRENT:
                return;

            case STALE:
                this.context.removeInferred(null, null, null, new Resource[] {});

            case POSSIBLY_INCOMPLETE:
                try {
                    LOGGER.debug("[{}] === Closure computation started ===", this.id);

                    long time = System.currentTimeMillis();
                    this.buffer = Lists.newArrayListWithCapacity(INITIAL_BUFFER_CAPACITY);
                    this.activeRules.addAll(RDFProInferencer.this.ruleset.getForwardRuleIDs());
                    this.lastBindings = null;
                    final long inferred = executeTask(
                            RDFProInferencer.this.ruleset.getClosurePlan(),
                            RDFProInferencer.this.rulesetBindings);
                    this.buffer = null;
                    time = System.currentTimeMillis() - time;

                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("[{}] === Closure computation completed after {} ms with "
                                + "{} new inferences ===",
                                new Object[] { this.id, time, inferred });
                    }

                } catch (final QueryEvaluationException ex) {
                    throw new RepositoryException(
                            "Closure computation failed: " + ex.getMessage(), ex);
                }
                break;

            default:
                throw new Error("Unknown closure status: " + closureStatus);
            }
            */
        	RuleEngine engine = RuleEngine.create(ruleset);
        	
        	final Buffer buffer = new Buffer(this.context.getValueFactory());
        	
        	engine.eval(getBuffer());
        }

        protected final List<Statement> getBuffer()
        {
            return this.buffer;
        }

       protected final long executeTask(final ClosureTask task, final BindingSet bindings)
                throws QueryEvaluationException, RepositoryException
        {
            BindingSet actualBindings = bindings;
            if (!task.getBindings().isEmpty()) {
                actualBindings = overrideBindings(bindings, task.getBindings());
            }

            if (!(task instanceof ClosureEvalTask)) {
                LOGGER.debug("[{}] --- Executing {} ---", this.id, task);
            }

            long result;
            if (task instanceof ClosureSequenceTask) {
                result = executeSequence((ClosureSequenceTask) task, actualBindings);
            } else if (task instanceof ClosureFixPointTask) {
                result = executeFixPoint((ClosureFixPointTask) task, actualBindings);
            } else if (task instanceof ClosureRepeatTask) {
                result = executeRepeat((ClosureRepeatTask) task, actualBindings);
            } else if (task instanceof ClosureEvalTask) {
                result = executeEval((ClosureEvalTask) task, actualBindings);
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
                final long inferred = executeTask(subTask, bindings);
                result += inferred;
            }
            return result;
        }

       
        protected long executeFixPoint(final ClosureFixPointTask task, final BindingSet bindings)
                throws QueryEvaluationException, RepositoryException
        {
            long result = 0L;
            int iteration = 1;

            while (true) {
                LOGGER.debug("[{}] Fix point iteration {} started", this.id, iteration);

                final long inferred = executeTask(task.getSubTask(), bindings);

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[{}] Fix point iteration {} completed with {} new inferences",
                            new Object[] { this.id, iteration, inferred });
                }

                result += inferred;
                ++iteration;

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
                iterationRange = Iterations.asList(this.context.query(task.getQuery(), null,
                        bindings, true, 0));
            } catch (final MalformedQueryException ex) {
                throw new Error("Unexpected exception: " + ex.getMessage(), ex);
            }

            long result = 0L;
            int iteration = 1;

            for (final BindingSet iterationBindings : iterationRange) {

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[{}] Repeat iteration {}/{} started", new Object[] { this.id,
                            iteration, iterationRange.size() });
                }

                final BindingSet actualBindings = overrideBindings(bindings, iterationBindings);
                final long inferred = executeTask(task.getSubTask(), actualBindings);

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[{}] Repeat iteration {}/{} completed with {} new inferences",
                            new Object[] { this.id, iteration, iterationRange.size(), inferred });
                }

                result += inferred;
                ++iteration;
            }

            return result;
        }

        /*protected long executeEval(final ClosureEvalTask task, final BindingSet bindings)
                throws QueryEvaluationException, RepositoryException
        {
            if (!bindings.equals(this.lastBindings)) {
                this.activeRules.addAll(RDFProInferencer.this.ruleset.getForwardRuleIDs());
                this.lastBindings = bindings;
            }

            final Buffer buffer = new Buffer(this.context.getValueFactory());

            final Queue<Resource> pendingRuleIDs = new ArrayDeque<Resource>();
            for (final Resource ruleID : task.getRuleIDs()) {
                final Rule rule = RDFProInferencer.this.ruleset.getRule(ruleID);
                if (!this.activeRules.contains(ruleID)) {
                    LOGGER.debug("[{}] Rule {} skipped because inactive", this.id, ruleID);
                } else if (rule.getCondition() != null
                        && !((Literal) Algebra.evaluateValueExpr(rule.getCondition(), bindings,
                                ValueFactoryImpl.getInstance())).booleanValue()) {
                    LOGGER.debug("[{}] Rule {} skipped because condition unsatisfied", this.id,
                            ruleID);
                } else {
                    this.activeRules.remove(ruleID);
                    pendingRuleIDs.offer(ruleID);
                }
            }

            if (pendingRuleIDs.isEmpty()) {
                return 0L;
            }

            final int numAuxiliaryTasks = this.context.getScheduler() == null ? 0 : Math.min(
                    RDFProInferencer.this.maxConcurrentRules, pendingRuleIDs.size()) - 1;
            final List<Future<?>> futures = Lists.newArrayListWithCapacity(numAuxiliaryTasks);
            for (int i = 0; i < numAuxiliaryTasks; ++i) {
                futures.add(this.context.getScheduler().submit(new Callable<Void>() {

                    @Override
                    public Void call() throws Exception
                    {
                        executeEvalHelper(pendingRuleIDs, bindings, buffer);
                        return null;
                    }

                }));
            }

            executeEvalHelper(pendingRuleIDs, bindings, buffer);

            for (final Future<?> future : futures) {
                try {
                    future.get();
                } catch (final ExecutionException ex) {
                    throw new RepositoryException("Rule evaluation failed: "
                            + ex.getCause().getMessage(), ex.getCause());
                } catch (final InterruptedException ex) {
                    throw new RepositoryException("Rule evaluation interrupted", ex);
                }
            }

            if (buffer.size() > 0) {
                LOGGER.debug("[{}] Flushing {} inferred statements to repository", this.id,
                        buffer.size());
                this.context.addInferred(buffer);
            }

            return buffer.size();
        }

        private void executeEvalHelper(final Queue<Resource> pendingRuleIDs,
                final BindingSet bindings, final Buffer buffer) throws QueryEvaluationException,
                RepositoryException
        {
            while (true) {
                Rule rule = null;
                synchronized (this) {
                    final Resource ruleID = pendingRuleIDs.poll();
                    if (ruleID == null) {
                        return;
                    }
                    rule = RDFProInferencer.this.ruleset.getRule(ruleID);
                }

                long time = System.currentTimeMillis();
                final int count = evaluateRule(rule, bindings, buffer);
                time = System.currentTimeMillis() - time;

                synchronized (this) {
                    RDFProInferencer.this.statistics.get(rule.getID()).recordActivations(count,
                            time);
                    if (count > 0) {
                        this.activeRules.addAll(Objects.firstNonNull(rule.getTriggeredRuleIDs(),
                                RDFProInferencer.this.ruleset.getForwardRuleIDs()));
                    }
                }

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[{}] Rule {} evaluated in {} ms, {} statements inferred",
                            new Object[] { this.id, rule, time, count });
                }
            }
        }*/ 

       /* protected int evaluateRule(final Rule rule, final BindingSet bindings, final Buffer buffer)
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
*/
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
            final String id = this.ruleID instanceof URI ? ((URI) this.ruleID).getLocalName()
                    : this.ruleID.stringValue();
            return String.format("%-20s %6d activation(s) %8d inf. statement(s) %8d ms total", id,
                    this.activations, this.statements, this.time);
        }

    }

}
