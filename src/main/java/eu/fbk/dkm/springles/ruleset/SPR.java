package eu.fbk.dkm.springles.ruleset;

import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;

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
    public static final URI RULESET = create("Ruleset");

    /** Object property <tt>:parameterizedBy</tt>. */
    public static final URI PARAMETERIZED_BY = create("parameterizedBy");

    /** String property <tt>:prologue</tt>. */
    public static final URI PROLOGUE = create("prologue");

    /** String property <tt>:macro</tt>. */
    public static final URI MACRO = create("macro");

    /** Object property <tt>:evalBackward</tt>. */
    public static final URI EVAL_BACKWARD = create("evalBackward");

    // UNSUPPORTED
    // /** Object property <tt>:evalForward</tt>. */
    // public static final URI EVAL_FORWARD = create("evalForward");

    /** Object property <tt>:closurePlan</tt>. */
    public static final URI CLOSURE_PLAN = create("closurePlan");

    // Ruleset parameters

    /** Class <tt>:Parameter</tt>. */
    public static final URI PARAMETER = create("Parameter");

    /** String property <tt>:name</tt>. */
    public static final URI NAME = create("name");

    /** String property <tt>:default</tt>. */
    public static final URI DEFAULT = create("default");

    // Rules

    /** Class <tt>:Rule</tt>. */
    public static final URI RULE = create("Rule");

    /** Auxiliary class <tt>:RuleList</tt>. */
    public static final URI RULE_LIST = create("RuleList");

    /** String property <tt>:condition</tt>. */
    public static final URI CONDITION = create("condition");

    /** String property <tt>:head</tt>. */
    public static final URI HEAD = create("head");

    /** String property <tt>:body</tt>. */
    public static final URI BODY = create("body");

    /** Object property <tt>:triggerOf</tt>. */
    public static final URI TRIGGER_OF = create("triggerOf");

    /** Object property <tt>:transform</tt>. */
    public static final URI TRANSFORM = create("transform");

    // Closure plan

    /** Class <tt>:ClosureTask</tt>. */
    public static final URI CLOSURE_TASK = create("ClosureTask");

    /** Class <tt>:ClosureTaskList</tt>. */
    public static final URI CLOSURE_TASK_LIST = create("ClosureTaskList");

    /** String property <tt>:bind</tt>. */
    public static final URI BIND = create("bind");

    /** Class <tt>:ClosureEvalTask</tt>. */
    public static final URI CLOSURE_EVAL_TASK = create("ClosureEvalTask");

    /** Object property <tt>:evalOf</tt>. */
    public static final URI EVAL_OF = create("evalOf");

    /** Class <tt>:ClosureSequenceTask</tt>. */
    public static final URI CLOSURE_SEQUENCE_TASK = create("ClosureSequenceTask");

    /** Object property <tt>:sequenceOf</tt>. */
    public static final URI SEQUENCE_OF = create("sequenceOf");

    /** Class <tt>:ClosureFixPointTask</tt>. */
    public static final URI CLOSURE_FIX_POINT_TASK = create("ClosureFixPointTask");

    /** Object property <tt>:fixPointOf</tt>. */
    public static final URI FIX_POINT_OF = create("fixPointOf");

    /** Class <tt>:ClosureRepeatTask</tt>. */
    public static final URI CLOSURE_REPEAT_TASK = create("ClosureRepeatTask");

    /** Object property <tt>:repeatOf</tt>. */
    public static final URI REPEAT_OF = create("repeatOf");

    /** String property <tt>:repeatOver</tt>. */
    public static final URI REPEAT_OVER = create("repeatOver");

    // Utilities and constructor

    private static URI create(final String localName)
    {
        return ValueFactoryImpl.getInstance().createURI(NAMESPACE, localName);
    }

    private SPR()
    {
    }

}
