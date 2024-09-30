package ai.dataprep.accio.utils;

import ai.dataprep.accio.plan.FedTableScan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;

import java.util.List;

public class CleanPlanExtractor implements ReflectiveVisitor {
    private final ReflectUtil.MethodDispatcher<RelNode> dispatcher;
    private RelOptCluster cluster;

    public RelNode extract(RelNode r) {
        // Unwrap the nodes from the graphs used by HepPlanner and VolcanoPlanner
        if (r instanceof HepRelVertex) {
            return extract(((HepRelVertex) r).getCurrentRel());
        }
        if (r instanceof RelSubset) {
            return extract(((RelSubset) r).getOriginal());
        }

        RelNode copy = r.copy(r.getTraitSet(), r.getInputs());

        List<RelNode> inputs = r.getInputs();
        for (int i = 0; i < inputs.size(); ++i) {
            RelNode child = inputs.get(i);
            child = extract(child);
            copy.replaceInput(i, child);
        }
        return dispatcher.invoke(copy);
    }


    public CleanPlanExtractor(RelOptCluster cluster) {
        this.cluster = cluster;
        dispatcher = ReflectUtil.createMethodDispatcher(RelNode.class, this, "replace",
                RelNode.class);
    }

    public RelNode replace(FedTableScan node) {
        return node.copyWithNewCluster(this.cluster);
    }

    public RelNode replace(Project node) {
        return LogicalProject.create(node.getInput(), node.getHints(), node.getProjects(), node.getRowType());
    }

    public RelNode replace(Filter node) {
        return LogicalFilter.create(node.getInput(), node.getCondition(), ImmutableSet.copyOf(node.getVariablesSet()));
    }

    public RelNode replace(Join node) {
        return LogicalJoin.create(node.getLeft(), node.getRight(), node.getHints(), node.getCondition(), node.getVariablesSet(), node.getJoinType(), node.isSemiJoinDone(), ImmutableList.copyOf(node.getSystemFieldList()));
    }

    public RelNode replace(Aggregate node) {
        return LogicalAggregate.create(node.getInput(), node.getHints(), node.getGroupSet(), node.getGroupSets(), node.getAggCallList());
    }

    public RelNode replace(Sort node) {
        return LogicalSort.create(node.getInput(), node.getCollation(), node.offset, node.fetch);
    }
}
