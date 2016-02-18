package eu.fbk.dkm.springles.ruleset;

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.openrdf.model.BNode;
import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.config.RepositoryConfigException;

import eu.fbk.dkm.internal.util.Macro;
import eu.fbk.dkm.internal.util.MacroExpander;
import eu.fbk.dkm.internal.util.RDFParseOptions;
import eu.fbk.dkm.internal.util.RDFSource;
import eu.fbk.dkm.internal.util.Selector;
import eu.fbk.dkm.internal.util.SparqlRenderer;
import eu.fbk.dkm.springles.Factory;

public final class Ruleset implements Cloneable
{

    @Nullable
    private URI id;

    @Nullable
    private URL url;

    @Nullable
    private String label;

    @Nullable
    private String baseURI;

    private Map<String, String> namespaces;

    private Map<String, Value> parameters;

    private List<Rule> rules;

    @Nullable
    private ClosureTask closurePlan;

    private Set<Resource> backwardRuleIDs;

    private boolean frozen;

    private transient Map<Resource, Rule> ruleIndex;

    private transient Set<Resource> forwardRuleIDs;

    private transient String digest;

    public Ruleset()
    {
        this(null, null, null, null, null);
    }

    public Ruleset(final URL url)
    {
        this(null, null, null, null, null);

        try {
            final Graph graph = RDFSource.deserializeFrom(url, new RDFParseOptions())
                    .streamToGraph();
            this.url = url;
            this.id = (URI) Iterators.getOnlyElement(graph.match(null, RDF.TYPE, SPR.RULESET))
                    .getSubject();
            parseRDF(graph);

        } catch (final Throwable ex) {
            throw new IllegalArgumentException("Unable to load ruleset definition " + url + ": ",
                    ex);
        }
    }

    public Ruleset(@Nullable final URI id, @Nullable final URL url,
            @Nullable final Iterable<? extends Rule> rules,
            @Nullable final ClosureTask closurePlan,
            @Nullable final Iterable<? extends Resource> backwardRuleIDs)
    {
        this.id = id;
        this.url = url;
        this.label = null;
        this.baseURI = null;
        this.namespaces = Maps.newHashMap();
        this.parameters = Maps.newHashMap();
        this.rules = rules == null ? Lists.<Rule>newArrayList() : Lists.newArrayList(rules);
        this.closurePlan = closurePlan;
        this.backwardRuleIDs = backwardRuleIDs == null ? Sets.<Resource>newHashSet() : Sets
                .newHashSet(backwardRuleIDs);
        this.frozen = false;

        this.ruleIndex = null;
        this.forwardRuleIDs = null;
        this.digest = null;
    }

    // PROPERTIES

    @Nullable
    public URI getID()
    {
        return this.id;
    }

    public void setID(@Nullable final URI id)
    {
        checkMutable();
        this.id = id;
    }

    @Nullable
    public URL getURL()
    {
        return this.url;
    }

    public void setURL(@Nullable final URL url)
    {
        checkMutable();
        this.url = url;
    }

    @Nullable
    public String getLabel()
    {
        return this.label;
    }

    public void setLabel(@Nullable final String label)
    {
        checkMutable();
        this.label = label;
    }

    public String getBaseURI()
    {
        return this.baseURI;
    }

    public void setBaseURI(final String baseURI)
    {
        checkMutable();
        this.baseURI = Strings.emptyToNull(baseURI);
    }

    public Map<String, String> getNamespaces()
    {
        return this.namespaces;
    }

    public void setNamespaces(@Nullable final Map<? extends String, ? extends String> namespaces)
    {
        checkMutable();
        this.namespaces = namespaces == null ? Maps.<String, String>newHashMap() : Maps
                .newHashMap(namespaces);
    }

    public Map<String, Value> getParameters()
    {
        return this.parameters;
    }

    public void setParameters(@Nullable final Map<? extends String, ? extends Value> parameters)
    {
        checkMutable();
        this.parameters = parameters == null ? Maps.<String, Value>newHashMap() : Maps
                .newHashMap(parameters);
    }

    public BindingSet getParameterBindings(@Nullable final BindingSet suppliedBindings)
    {
        final MapBindingSet result = new MapBindingSet();

        for (final String variable : this.parameters.keySet()) {
            final Value defaultValue = this.parameters.get(variable);
            final Value assignedValue = suppliedBindings == null ? null : suppliedBindings
                    .getValue(variable);

            if (defaultValue == null) {
                if (assignedValue != null) {
                    result.addBinding(variable, assignedValue);
                } else {
                    throw new IllegalArgumentException("Missing binding for mandatory parameter "
                            + variable);
                }
            } else {
                if (assignedValue != null) {
                    if (defaultValue instanceof Resource && !(assignedValue instanceof Resource)) {
                        throw new IllegalArgumentException("Expected an RDF resource "
                                + "for parameter " + variable + ", got " + assignedValue);
                    } else if (defaultValue instanceof Literal
                            && !(assignedValue instanceof Literal)) {
                        throw new IllegalArgumentException("Expected an RDF literal "
                                + "for parameter " + variable + ", got " + assignedValue);
                    }
                    result.addBinding(variable, assignedValue);
                } else {
                    result.addBinding(variable, defaultValue);
                }
            }
        }

        return result;
    }

    public List<Rule> getRules()
    {
        return this.rules;
    }

    public void setRules(@Nullable final Iterable<? extends Rule> rules)
    {
        checkMutable();
        this.rules = rules == null ? Lists.<Rule>newArrayList() : Lists.newArrayList(rules);
    }

    @Nullable
    public Rule getRule(final Resource id)
    {
        Preconditions.checkNotNull(id);
        return getRuleIndex().get(id);
    }

    private Map<Resource, Rule> getRuleIndex()
    {
        Map<Resource, Rule> ruleIndex = this.ruleIndex;
        if (ruleIndex == null) {
            final Map<Resource, Rule> map = Maps.newHashMap();
            for (final Rule rule : this.rules) {
                final Rule oldRule = map.put(rule.getID(), rule);
                if (oldRule != null && oldRule != rule) {
                    throw new IllegalStateException("Multiple rules with ID " + rule.getID()
                            + ":\n" + oldRule + "\n" + rule);
                }
            }
            ruleIndex = ImmutableMap.copyOf(map);
            this.ruleIndex = this.frozen ? ruleIndex : null;
        }
        return ruleIndex;
    }

    @Nullable
    public ClosureTask getClosurePlan()
    {
        return this.closurePlan;
    }

    public void setClosurePlan(@Nullable final ClosureTask closurePlan)
    {
        checkMutable();
        this.closurePlan = closurePlan;
    }

    public Set<Resource> getForwardRuleIDs()
    {
        Set<Resource> result = this.forwardRuleIDs;
        if (result == null) {
            result = Sets.newHashSet();
            if (this.closurePlan != null) {
                extractRuleIDs(this.closurePlan, result);
            }
            this.forwardRuleIDs = isFrozen() ? result : null;
        }
        return result;
    }

    private static void extractRuleIDs(final ClosureTask node, final Set<Resource> ruleIDs)
    {
        if (node instanceof ClosureEvalTask) {
            ruleIDs.addAll(((ClosureEvalTask) node).getRuleIDs());
        } else if (node instanceof ClosureSequenceTask) {
            for (final ClosureTask arg : ((ClosureSequenceTask) node).getSubTasks()) {
                extractRuleIDs(arg, ruleIDs);
            }
        } else {
            ClosureTask arg = null;
            if (node instanceof ClosureFixPointTask) {
                arg = ((ClosureFixPointTask) node).getSubTask();
            } else if (node instanceof ClosureRepeatTask) {
                arg = ((ClosureRepeatTask) node).getSubTask();
            }
            if (arg != null) {
                extractRuleIDs(arg, ruleIDs);
            }
        }
    }

    public Set<Resource> getBackwardRuleIDs()
    {
        return this.backwardRuleIDs;
    }

    public void setBackwardRuleIDs(@Nullable final Iterable<? extends Resource> backwardRuleIDs)
    {
        Preconditions.checkNotNull(backwardRuleIDs);
        checkMutable();

        this.backwardRuleIDs = backwardRuleIDs == null ? Sets.<Resource>newHashSet() : Sets
                .newHashSet(backwardRuleIDs);
    }

    // SERIALIZATION AND DESERIALIZATION IN RDF

 /*   public Resource emitRDF(final Graph graph)
    {
        Preconditions.checkNotNull(graph);

        final ValueFactory vf = graph.getValueFactory();
        final Resource id = this.id != null ? this.id : vf.createBNode();

        graph.add(id, RDF.TYPE, SPR.RULESET);

        if (this.label != null) {
            graph.add(id, RDFS.LABEL, vf.createLiteral(this.label, XMLSchema.STRING));
        }

        if (this.baseURI != null || !this.namespaces.isEmpty()) {
            final StringBuilder builder = new StringBuilder();
            if (this.baseURI != null) {
                builder.append("BASE ")
                        .append(SparqlRenderer.render(new URIImpl(this.baseURI)).toString())
                        .append("\n");
            }
            for (final Map.Entry<String, String> entry : this.namespaces.entrySet()) {
                builder.append("PREFIX ").append(entry.getKey()).append(": ")
                        .append(SparqlRenderer.render(new URIImpl(entry.getValue())).toString())
                        .append("\n");
            }
            graph.add(id, SPR.PROLOGUE, vf.createLiteral(builder.toString(), XMLSchema.STRING));
        }

        for (final Map.Entry<String, Value> entry : this.parameters.entrySet()) {
            final BNode node = vf.createBNode();
            final Value defaultValue = entry.getValue() instanceof URI ? vf.createLiteral(entry
                    .getValue().stringValue(), XMLSchema.ANYURI) : entry.getValue();
            graph.add(id, SPR.PARAMETERIZED_BY, node);
            graph.add(node, SPR.NAME, vf.createLiteral(entry.getKey(), XMLSchema.STRING));
            graph.add(node, SPR.DEFAULT, defaultValue);
        }

        if (this.closurePlan != null) {
            final Resource taskID = this.closurePlan.emitRDF(graph, this.baseURI, this.namespaces);
            graph.add(id, SPR.CLOSURE_PLAN, taskID);
        }

        for (final Resource ruleID : this.backwardRuleIDs) {
            graph.add(id, SPR.EVAL_BACKWARD, ruleID);
        }

        return id;
    }*/

    public void parseRDF(final Graph graph) throws MalformedQueryException
    {
        checkMutable();
        Preconditions.checkState(this.id != null);

        Selector s = Selector.select(graph, this.id);

        final List<Macro> macros = Lists.newArrayList();
        for (final String definition : s.getAll(SPR.MACRO, String.class)) {
            for (final Macro macro : Macro.read(definition)) {
                for (final String argName : macro.getArgs()) {
                    if (Pattern.compile("[\\?\\$]" + Pattern.quote(argName) + "[^a-zA-Z_0-9]")
                            .matcher(macro.getTemplate()).find()) {
                        throw new IllegalArgumentException("Invalid macro definition (argument '"
                                + argName + "' used as SPARQL variable):\n" + definition);
                    }
                }
                macros.add(macro);
            }
        }
        final MacroExpander expander = new MacroExpander(macros);

        final Graph expandedGraph = new GraphImpl();
        final ValueFactory vf = expandedGraph.getValueFactory();
        for (final Statement stmt : graph) {
            final URI pred = stmt.getPredicate();
            if (SPR.CONDITION.equals(pred) || SPR.HEAD.equals(pred) || SPR.BODY.equals(pred)
                    || SPR.BIND.equals(pred) || SPR.REPEAT_OVER.equals(pred)) {
                final Literal obj = vf.createLiteral(
                        expander.apply(stmt.getObject().stringValue()), XMLSchema.STRING);
                expandedGraph.add(vf.createStatement(stmt.getSubject(), pred, obj));
            } else {
                expandedGraph.add(stmt);
            }
        }
        s = Selector.select(expandedGraph, this.id);

        if (!s.isEmpty(RDFS.LABEL)) {
            this.label = s.get(RDFS.LABEL, String.class);
        }

        final String prologue = s.get(SPR.PROLOGUE, String.class, null);
        if (prologue != null) {
            try {
                this.baseURI = null;
                this.namespaces.putAll(((ParsedGraphQuery) new SPARQLParser().parseQuery(prologue
                        + "\nCONSTRUCT {} WHERE {}", null)).getQueryNamespaces());
                final String text = prologue.replace(" ", "");
                int start = text.toUpperCase().indexOf("BASE<");
                if (start >= 0) {
                    start += 5;
                    final int end = text.indexOf('>', start);
                    this.baseURI = text.substring(start, end);
                }
            } catch (final MalformedQueryException ex) {
                throw new IllegalArgumentException("Malformed namespace declaration: "
                        + ex.getMessage() + "\n" + prologue, ex);
            }
        }

        if (!s.isEmpty(SPR.PARAMETERIZED_BY)) {
            for (final Resource resource : s.getAll(SPR.PARAMETERIZED_BY, Resource.class)) {
                final Selector p = Selector.select(expandedGraph, resource);
                final String variable = p.get(SPR.NAME, String.class).replace("?", "");
                Preconditions.checkArgument(!this.parameters.containsKey(variable),
                        "Duplicate parameter: " + variable);
                Value value = p.get(SPR.DEFAULT, Value.class, null);
                if (value instanceof Literal
                        && XMLSchema.ANYURI.equals(((Literal) value).getDatatype())) {
                    value = expandedGraph.getValueFactory().createURI(value.stringValue());
                }
                this.parameters.put(variable, value);
            }
        }

        final Set<Resource> ruleIDs = Sets.newHashSet();
        if (!s.isEmpty(SPR.EVAL_BACKWARD)) {
            final List<Resource> ids = s.getAll(SPR.EVAL_BACKWARD, Resource.class);
            this.backwardRuleIDs = Sets.newHashSet(ids);
            ruleIDs.addAll(ids);
        }

        this.closurePlan = ClosureTask.parseRDF(expandedGraph, this.baseURI, this.namespaces,
                s.get(SPR.CLOSURE_PLAN, Resource.class));
        extractRuleIDs(this.closurePlan, ruleIDs);

        ruleIDs.removeAll(getRuleIndex().keySet());
        for (final Resource ruleID : ruleIDs) {
            this.rules.add(Rule.parseRDF(expandedGraph, this.baseURI, this.namespaces, ruleID));
        }
    }

    public static Ruleset parseRDF(final Graph graph, @Nullable final URI rulesetID)
            throws MalformedQueryException
    {
        final URI id = rulesetID != null ? rulesetID : (URI) Iterators.getOnlyElement(
                graph.match(null, RDF.TYPE, SPR.RULESET)).getSubject();
        final Ruleset ruleset = new Ruleset();
        ruleset.setID(id);
        ruleset.parseRDF(graph);
        return ruleset;
    }

    // VALIDATION

    public void validate()
    {
        validate(this.id != null, "missing ruleset ID");
        validate(!this.parameters.containsKey(null), "null parameter name");
        validate(this.closurePlan != null, "missing closure plan");

        final Set<Resource> missingRules = Sets.difference(
                Sets.union(this.backwardRuleIDs, getForwardRuleIDs()), getRuleIndex().keySet());
        validate(missingRules.isEmpty(), "undefined rules " + missingRules);

        this.closurePlan.validate();
        for (final Rule rule : this.rules) {
            rule.validate();
        }
    }

    private void validate(final boolean condition, final String message)
    {
        if (!condition) {
            throw new IllegalStateException("Invalid ruleset"
                    + (this.id == null ? "" : " " + this.id) + ": " + message);
        }
    }

    // FREEZING AND CLONING

    public boolean isFrozen()
    {
        return this.frozen;
    }

    public void freeze()
    {
        if (!this.frozen) {
            this.frozen = true;
            this.parameters = ImmutableMap.copyOf(this.parameters);
            this.rules = ImmutableList.copyOf(this.rules);
            for (final Rule rule : this.rules) {
                rule.freeze();
            }
            if (this.closurePlan != null) {
                this.closurePlan.freeze();
            }
            this.backwardRuleIDs = ImmutableSet.copyOf(this.backwardRuleIDs);
        }
    }

    @Override
    public Ruleset clone()
    {
        try {
            final Ruleset clone = (Ruleset) super.clone();
            clone.parameters = Maps.newHashMap(this.parameters);
            clone.rules = Lists.newArrayListWithCapacity(this.rules.size());
            for (final Rule rule : this.rules) {
                clone.rules.add(rule.clone());
            }
            clone.closurePlan = this.closurePlan == null ? null : this.closurePlan.clone();
            clone.backwardRuleIDs = Sets.newHashSet(this.backwardRuleIDs);
            clone.frozen = false;
            clone.ruleIndex = null;
            clone.forwardRuleIDs = null;
            return clone;

        } catch (final CloneNotSupportedException ex) {
            throw new Error("Unexpected exception: " + ex.getMessage(), ex);
        }
    }

    private void checkMutable()
    {
        if (this.frozen) {
            throw new IllegalStateException("Cannot modify ruleset " + this.id
                    + "; must clone it before");
        }
    }

    // COMPARISON AND HASHING

    @Override
    public boolean equals(@Nullable final Object object)
    {
        if (object == this) {
            return true;
        }
        if (object == null || !(object instanceof Ruleset)) {
            return false;
        }
        final Ruleset other = (Ruleset) object;
        return Objects.equal(this.id, other.id) //
                && this.parameters.equals(other.parameters)
                && getRuleIndex().equals(other.getRuleIndex())
                && Objects.equal(this.closurePlan, other.closurePlan)
                && Objects.equal(this.backwardRuleIDs, other.backwardRuleIDs);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(this.id, this.parameters, getRuleIndex(), this.closurePlan,
                this.backwardRuleIDs);
    }

    public String digest()
    {
        String digest = this.digest;
        if (digest == null) {
            final Hasher hasher = Hashing.md5().newHasher();
            for (final String variable : Ordering.natural().sortedCopy(this.parameters.keySet())) {
                hasher.putUnencodedChars(variable).putUnencodedChars(this.parameters.get(variable).stringValue());
            }
            final List<String> ruleDigests = Lists.newArrayList();
            for (final Rule rule : this.rules) {
                ruleDigests.add(rule.digest());
            }
            Collections.sort(ruleDigests);
            hasher.putUnencodedChars(Joiner.on(" ").join(ruleDigests));
            hasher.putUnencodedChars(this.closurePlan == null ? " " : this.closurePlan.digest());
            digest = hasher.hash().toString();
            this.digest = this.frozen ? digest : null;
        }
        return digest;
    }

    // STRING REPRESENTATION

    @Override
    public String toString()
    {
        return (this.id == null ? "unnamed ruleset" : this.id.stringValue())
                + (this.label == null ? "" : " - " + this.label);
    }

    // INTEGRATION WITH FACTORY MECHANISM

    static Factory<Ruleset> getFactory(final Graph graph, final Resource node)
            throws RepositoryConfigException
    {
        Preconditions.checkArgument(node instanceof URI,
                "invalid ruleset identifier (must be a URI): " + node);

        try {
            final Ruleset ruleset = parseRDF(graph, (URI) node);
            ruleset.validate();
            ruleset.freeze();

            return new Factory<Ruleset>() {

                @Override
                public Ruleset create() throws RepositoryException
                {
                    return ruleset;
                }

            };

        } catch (final Throwable ex) {
            throw new RepositoryConfigException("Invalid ruleset: " + ex.getMessage(), ex);
        }
    }

}
