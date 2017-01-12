package eu.fbk.dkm.internal.util;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.openrdf.model.Value;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.Distinct;
import org.openrdf.query.algebra.EmptySet;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Group;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.Order;
import org.openrdf.query.algebra.OrderElem;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.Reduced;
import org.openrdf.query.algebra.Slice;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

public class SparqlQuery
{

    public enum QueryForm
    {
        SELECT, CONSTRUCT, ASK
    }

    public enum SelectModifier
    {
        DISTINCT, REDUCED
    };

    private QueryForm queryForm;

    private SelectModifier selectModifier;

    private List<ProjectionElem> selectList;

    private TupleExpr constructExpression;

    private Dataset fromDataset;

    private TupleExpr whereExpression;

    private List<ProjectionElem> groupByList;

    private ValueExpr havingCondition;

    private List<OrderElem> orderByList;

    private Long sliceOffset;

    private Long sliceLimit;

    public SparqlQuery()
    {
        this.queryForm = QueryForm.SELECT;
        this.selectModifier = null;
        this.selectList = Lists.newArrayList();
        this.constructExpression = null;
        this.fromDataset = null;
        this.whereExpression = null;
        this.groupByList = Lists.newArrayList();
        this.havingCondition = null;
        this.orderByList = Lists.newArrayList();
        this.sliceOffset = null;
        this.sliceLimit = null;
    }

    public boolean hasSelectClause()
    {
        return this.queryForm == QueryForm.SELECT;
    }

    public boolean hasConstructClause()
    {
        return this.queryForm == QueryForm.CONSTRUCT;
    }

    public boolean hasWhereClause()
    {
        return this.whereExpression != null;
    }

    public boolean hasGroupByClause()
    {
        return !this.groupByList.isEmpty();
    }

    public boolean hasHavingClause()
    {
        return this.havingCondition != null;
    }

    public boolean hasOrderByClause()
    {
        return !this.orderByList.isEmpty();
    }

    public boolean hasSliceClause()
    {
        return this.sliceOffset != null || this.sliceLimit != null;
    }

    public QueryForm getQueryForm()
    {
        return this.queryForm;
    }

    public void setQueryForm(final QueryForm queryForm)
    {
        this.queryForm = queryForm;
    }

    public SelectModifier getSelectModifier()
    {
        return this.selectModifier;
    }

    public void setSelectModifier(final SelectModifier selectModifier)
    {
        this.selectModifier = selectModifier;
    }

    public List<ProjectionElem> getSelectList()
    {
        return this.selectList;
    }

    public void setSelectList(final Iterable<? extends ProjectionElem> selectList)
    {
        this.selectList = selectList != null ? Lists.newArrayList(selectList) : Lists
                .<ProjectionElem>newArrayList();
    }

    public TupleExpr getConstructExpression()
    {
        return this.constructExpression;
    }

    public void setConstructExpression(final TupleExpr constructExpression)
    {
        this.constructExpression = constructExpression;
    }

    public Dataset getFromDataset()
    {
        return this.fromDataset;
    }

    public void setFromDataset(final Dataset fromDataset)
    {
        this.fromDataset = fromDataset;
    }

    public TupleExpr getWhereExpression()
    {
        return this.whereExpression;
    }

    public void setWhereExpression(final TupleExpr whereExpression)
    {
        this.whereExpression = whereExpression;
    }

    public List<ProjectionElem> getGroupByList()
    {
        return this.groupByList;
    }

    public void setGroupByList(final Iterable<? extends ProjectionElem> groupByList)
    {
        this.groupByList = groupByList != null ? Lists.newArrayList(groupByList) : Lists
                .<ProjectionElem>newArrayList();
    }

    public ValueExpr getHavingCondition()
    {
        return this.havingCondition;
    }

    public void setHavingCondition(final ValueExpr havingCondition)
    {
        this.havingCondition = havingCondition;
    }

    public List<OrderElem> getOrderByList()
    {
        return this.orderByList;
    }

    public void setOrderByList(final List<OrderElem> orderByList)
    {
        this.orderByList = orderByList != null ? Lists.newArrayList(orderByList) : Lists
                .<OrderElem>newArrayList();
    }

    public Long getSliceOffset()
    {
        return this.sliceOffset;
    }

    public void setSliceOffset(final Long sliceOffset)
    {
        this.sliceOffset = sliceOffset;
    }

    public Long getSliceLimit()
    {
        return this.sliceLimit;
    }

    public void setSliceLimit(final Long sliceLimit)
    {
        this.sliceLimit = sliceLimit;
    }

    @Override
    public String toString()
    {
        final StringBuilder builder = new StringBuilder();

        if (this.queryForm == QueryForm.SELECT) {
            builder.append("SELECT")
                    .append(this.selectModifier != null ? " " + this.selectModifier : "")
                    .append("\n");
            for (final ProjectionElem elem : this.selectList) {
                printTree(elem, builder, "    ", "  ");
            }
        } else if (this.queryForm == QueryForm.CONSTRUCT) {
            builder.append("CONSTRUCT\n");
            printTree(this.constructExpression, builder, "    ", "  ");
        } else if (this.queryForm == QueryForm.ASK) {
            builder.append("ASK\n");
        }

        if (this.fromDataset != null) {
            builder.append("FROM\n").append(this.fromDataset);
        }

        if (this.whereExpression != null) {
            builder.append("WHERE\n");
            printTree(this.whereExpression, builder, "    ", "  ");
        }

        if (!this.groupByList.isEmpty()) {
            builder.append("GROUP BY\n");
            for (final ProjectionElem elem : this.groupByList) {
                printTree(elem, builder, "    ", "  ");
            }
        }

        if (this.havingCondition != null) {
            builder.append("HAVING\n");
            printTree(this.havingCondition, builder, "    ", "  ");
        }

        if (!this.orderByList.isEmpty()) {
            builder.append("ORDER BY\n");
            for (final OrderElem elem : this.orderByList) {
                printTree(elem, builder, "    ", "  ");
            }
        }

        if (this.queryForm != QueryForm.ASK && this.sliceOffset != null) {
            builder.append("OFFSET ").append(this.sliceOffset).append("\n");
        }
        if (this.queryForm != QueryForm.ASK && this.sliceLimit != null) {
            builder.append("LIMIT ").append(this.sliceLimit).append("\n");
        }

        return builder.toString();
    }

    private static void printTree(final QueryModelNode node, final StringBuilder builder,
            final String linePrefix, final String indentString)
    {
        node.visit(new QueryModelVisitorBase<RuntimeException>() {

            private int level = 0;

            @Override
            protected void meetNode(final QueryModelNode node) throws RuntimeException
            {
                builder.append(linePrefix);
                for (int i = 0; i < this.level; ++i) {
                    builder.append(indentString);
                }
                builder.append(node.getSignature());
                if (node instanceof ProjectionElem
                        && ((ProjectionElem) node).getSourceExpression() != null) {
                    builder.append(" AS\n");
                    ++this.level;
                    ((ProjectionElem) node).getSourceExpression().getExpr().visit(this);
                    --this.level;
                } else {
                    builder.append("\n");
                }
                ++this.level;
                super.meetNode(node);
                --this.level;
            }

        });
    }

    public static SparqlQuery fromTupleExpr(final TupleExpr rootNode, final Dataset dataset)
    {
        Preconditions.checkNotNull(rootNode);

        if (rootNode instanceof EmptySet) {
            final SparqlQuery query = new SparqlQuery();
            query.setQueryForm(QueryForm.CONSTRUCT);
            query.setConstructExpression(rootNode);
            query.setWhereExpression(rootNode);
            return query;
        }

        final List<UnaryTupleOperator> nodes = extractQueryNodes(rootNode, false);

        final SparqlQuery query = new SparqlQuery();
        QueryForm queryForm = null;
        query.setWhereExpression(nodes.get(nodes.size() - 1).getArg());
        query.setFromDataset(dataset);

        for (final UnaryTupleOperator node : nodes) {

            if (node instanceof Distinct) {
                query.setSelectModifier(SelectModifier.DISTINCT);

            } else if (node instanceof Reduced) {
                query.setSelectModifier(SelectModifier.REDUCED);

            } else if (node instanceof Projection) {
                final Map<String, ExtensionElem> extensions = extractExtensions(node);
                final List<ProjectionElem> projections = ((Projection) node)
                        .getProjectionElemList().getElements();
                final boolean isConstruct = projections.size() >= 3
                        && "subject".equals(projections.get(0).getTargetName())
                        && "predicate".equals(projections.get(1).getTargetName())
                        && "object".equals(projections.get(2).getTargetName())
                        && (projections.size() == 3 || projections.size() == 4
                                && "context".equals(projections.get(3).getTargetName()));
                if (isConstruct) {
                    queryForm = QueryForm.CONSTRUCT;
                    query.setConstructExpression(extractConstructExpression(extensions,
                            Collections.singleton(((Projection) node).getProjectionElemList())));
                } else {
                    queryForm = QueryForm.SELECT;
                    for (final ProjectionElem projection : projections) {
                        final String variable = projection.getTargetName();
                        final ExtensionElem extension = extensions.get(variable);
                        final ProjectionElem newProjection = new ProjectionElem();
                        newProjection.setTargetName(variable);
                        newProjection.setSourceExpression(extension);
                        newProjection.setSourceName(extension == null
                                || !(extension.getExpr() instanceof Var) ? variable
                                : ((Var) extension.getExpr()).getName());
                        query.getSelectList().add(newProjection);
                    }
                }

            } else if (node instanceof MultiProjection) {
                query.setQueryForm(QueryForm.CONSTRUCT);
                query.setConstructExpression(extractConstructExpression(extractExtensions(node),
                        ((MultiProjection) node).getProjections()));

            } else if (node instanceof Group) {
                final Group group = (Group) node;
                final Map<String, ExtensionElem> extensions = extractExtensions(group.getArg());
                for (final String variableName : group.getGroupBindingNames()) {
                    final ExtensionElem extension = extensions.get(variableName);
                    final ProjectionElem projection = new ProjectionElem();
                    projection.setTargetName(variableName);
                    projection.setSourceExpression(extension);
                    projection.setSourceName(extension == null
                            || !(extension.getExpr() instanceof Var) ? variableName
                            : ((Var) extension.getExpr()).getName());
                    query.getGroupByList().add(projection);
                }

            } else if (node instanceof Order) {
                query.setOrderByList(((Order) node).getElements());

            } else if (node instanceof Slice) {
                final Slice slice = (Slice) node;
                query.setSliceOffset(slice.getOffset() < 0 ? null : slice.getOffset());
                query.setSliceLimit(slice.getLimit() <= 0 ? null : slice.getLimit());
                if (queryForm == null && slice.getOffset() == 0 && slice.getLimit() == 1) {
                    queryForm = QueryForm.ASK;
                }

            } else if (node instanceof Filter) {
                query.setHavingCondition(((Filter) node).getCondition());
            }
        }

        query.setQueryForm(Objects.firstNonNull(queryForm, QueryForm.CONSTRUCT));
        if (query.getQueryForm() == QueryForm.CONSTRUCT && query.getConstructExpression() == null) {
            query.setConstructExpression(new EmptySet());
        }
        return query;
    }

    private static List<UnaryTupleOperator> extractQueryNodes(final TupleExpr rootNode,
            final boolean haltOnGroup)
    {
        final List<UnaryTupleOperator> nodes = Lists.newArrayList();

        TupleExpr queryNode = rootNode;
        while (queryNode instanceof UnaryTupleOperator) {
            nodes.add((UnaryTupleOperator) queryNode);
            queryNode = ((UnaryTupleOperator) queryNode).getArg();
        }

        boolean modifierFound = false;
        boolean projectionFound = false;
        boolean groupFound = false;
        boolean orderFound = false;
        boolean sliceFound = false;

        int index = 0;
        while (index < nodes.size()) {
            final UnaryTupleOperator node = nodes.get(index);
            if ((node instanceof Distinct || node instanceof Reduced) && !modifierFound
                    && !projectionFound) {
                modifierFound = true;

            } else if ((node instanceof Projection || node instanceof MultiProjection)
                    && !projectionFound) {
                projectionFound = true;

            } else if (node instanceof Group && !groupFound && !haltOnGroup) {
                groupFound = true;

            } else if (node instanceof Order && !orderFound) {
                orderFound = true;

            } else if (node instanceof Slice && !sliceFound) {
                sliceFound = true;

            } else if (node instanceof Filter && !groupFound && !haltOnGroup) {
                int i = index + 1;
                for (; i < nodes.size() && nodes.get(i) instanceof Extension; ++i) {
                }
                if (i < nodes.size() && nodes.get(i) instanceof Group) {
                    groupFound = true;
                    index = i;
                } else {
                    break;
                }

            } else if (!(node instanceof QueryRoot) && !(node instanceof Extension)) {
                break;
            }
            ++index;
        }

        return nodes.subList(0, index);
    }

    private static Map<String, ExtensionElem> extractExtensions(final TupleExpr rootNode)
    {
        final Map<String, ExtensionElem> map = Maps.newHashMap();
        for (final UnaryTupleOperator node : extractQueryNodes(rootNode, true)) {
            if (node instanceof Extension) {
                for (final ExtensionElem elem : ((Extension) node).getElements()) {
                    final String variable = elem.getName();
                    final ValueExpr expression = elem.getExpr();
                    if (!(expression instanceof Var)
                            || !((Var) expression).getName().equals(variable)) {
                        map.put(variable, elem);
                    }
                }
            }
        }
        return map;
    }

    private static TupleExpr extractConstructExpression(
            final Map<String, ExtensionElem> extensions,
            final Iterable<? extends ProjectionElemList> multiProjections)
    {
        TupleExpr expression = null;
        for (final ProjectionElemList projections : multiProjections) {
            final Var subj = extractConstructVar(extensions, projections.getElements().get(0));
            final Var pred = extractConstructVar(extensions, projections.getElements().get(1));
            final Var obj = extractConstructVar(extensions, projections.getElements().get(2));
            final Var ctx = projections.getElements().size() < 4 ? null : extractConstructVar(
                    extensions, projections.getElements().get(3));
            final StatementPattern pattern = new StatementPattern(
                    ctx == null ? Scope.DEFAULT_CONTEXTS : Scope.NAMED_CONTEXTS, subj, pred, obj,
                    ctx);
            expression = expression == null ? pattern : new Join(expression, pattern);
        }
        return expression;
    }

    private static Var extractConstructVar(final Map<String, ExtensionElem> extensions,
            final ProjectionElem projection)
    {
        final String name = projection.getSourceName();
        final ExtensionElem extension = extensions.get(name);
        if (extension == null) {
            final Var var = new Var(name);
            var.setAnonymous(name.startsWith("-anon-"));
            return var;
        } else {
            final Value value = ((ValueConstant) extension.getExpr()).getValue();
            return new Var(name, value);
        }
    }

}
