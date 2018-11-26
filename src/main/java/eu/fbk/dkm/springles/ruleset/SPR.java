package eu.fbk.dkm.springles.ruleset;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Springles ruleset vocabulary.
 *
 * @apiviz.stereotype static
 */
public final class SPR
{

    // Namespace

    /** Schema namespace <tt>http://dkm.fbk.eu/springles#</tt>. */
    public static final String NAMESPACE = "http://dkm.fbk.eu/springles/ruleset#";

    // Ruleset

    /** Class <tt>:Ruleset</tt>. */
    public static final IRI RULESET = SPR.create("Ruleset");

    /** Object property <tt>:parameterizedBy</tt>. */
    public static final IRI PARAMETERIZED_BY = SPR.create("parameterizedBy");

    /** String property <tt>:prologue</tt>. */
    public static final IRI PROLOGUE = SPR.create("prologue");

    /** String property <tt>:macro</tt>. */
    public static final IRI MACRO = SPR.create("macro");

    /** Object property <tt>:evalBackward</tt>. */
    public static final IRI EVAL_BACKWARD = SPR.create("evalBackward");

    // UNSUPPORTED
    // /** Object property <tt>:evalForward</tt>. */
    // public static final URI EVAL_FORWARD = create("evalForward");

    /** Object property <tt>:closurePlan</tt>. */
    public static final IRI CLOSURE_PLAN = SPR.create("closurePlan");

    // Ruleset parameters

    /** Class <tt>:Parameter</tt>. */
    public static final IRI PARAMETER = SPR.create("Parameter");

    /** String property <tt>:name</tt>. */
    public static final IRI NAME = SPR.create("name");

    /** String property <tt>:default</tt>. */
    public static final IRI DEFAULT = SPR.create("default");

    // Rules

    /** Class <tt>:Rule</tt>. */
    public static final IRI RULE = SPR.create("Rule");

    /** Auxiliary class <tt>:RuleList</tt>. */
    public static final IRI RULE_LIST = SPR.create("RuleList");

    /** String property <tt>:condition</tt>. */
    public static final IRI CONDITION = SPR.create("condition");

    /** String property <tt>:head</tt>. */
    public static final IRI HEAD = SPR.create("head");

    /** String property <tt>:body</tt>. */
    public static final IRI BODY = SPR.create("body");

    /** Object property <tt>:triggerOf</tt>. */
    public static final IRI TRIGGER_OF = SPR.create("triggerOf");

    /** Object property <tt>:transform</tt>. */
    public static final IRI TRANSFORM = SPR.create("transform");

    // Closure plan

    /** Class <tt>:ClosureTask</tt>. */
    public static final IRI CLOSURE_TASK = SPR.create("ClosureTask");

    /** Class <tt>:ClosureTaskList</tt>. */
    public static final IRI CLOSURE_TASK_LIST = SPR.create("ClosureTaskList");

    /** String property <tt>:bind</tt>. */
    public static final IRI BIND = SPR.create("bind");

    /** Class <tt>:ClosureEvalTask</tt>. */
    public static final IRI CLOSURE_EVAL_TASK = SPR.create("ClosureEvalTask");

    /** Object property <tt>:evalOf</tt>. */
    public static final IRI EVAL_OF = SPR.create("evalOf");

    /** Class <tt>:ClosureSequenceTask</tt>. */
    public static final IRI CLOSURE_SEQUENCE_TASK = SPR.create("ClosureSequenceTask");

    /** Object property <tt>:sequenceOf</tt>. */
    public static final IRI SEQUENCE_OF = SPR.create("sequenceOf");

    /** Class <tt>:ClosureFixPointTask</tt>. */
    public static final IRI CLOSURE_FIX_POINT_TASK = SPR.create("ClosureFixPointTask");

    /** Object property <tt>:fixPointOf</tt>. */
    public static final IRI FIX_POINT_OF = SPR.create("fixPointOf");

    /** Class <tt>:ClosureRepeatTask</tt>. */
    public static final IRI CLOSURE_REPEAT_TASK = SPR.create("ClosureRepeatTask");

    /** Object property <tt>:repeatOf</tt>. */
    public static final IRI REPEAT_OF = SPR.create("repeatOf");

    /** String property <tt>:repeatOver</tt>. */
    public static final IRI REPEAT_OVER = SPR.create("repeatOver");

    // Utilities and constructor

    private static IRI create(final String localName)
    {
        return SimpleValueFactory.getInstance().createIRI(SPR.NAMESPACE, localName);
    }

    private SPR()
    {
    }

}
