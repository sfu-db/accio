package ai.dataprep.accio.utils;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedRelRules;
import ai.dataprep.accio.plan.FedTableScan;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;

import java.util.List;

public class PhysicalPlanExtractor implements ReflectiveVisitor {
    private final ReflectUtil.MethodDispatcher<RelNode> dispatcher;
    private FedConvention convention;

    public RelNode extract(RelNode r) {
        // Unwrap the nodes from the graphs used by HepPlanner and VolcanoPlanner
        if (r instanceof HepRelVertex) {
            return extract(((HepRelVertex) r).getCurrentRel());
        }
        if (r instanceof RelSubset) {
            return extract(((RelSubset) r).getOriginal());
        }

        List<RelNode> inputs = r.getInputs();
        for (int i = 0; i < inputs.size(); ++i) {
            RelNode child = inputs.get(i);
            child = extract(child);
            r.replaceInput(i, child);
        }
        return dispatcher.invoke(r);
    }


    public PhysicalPlanExtractor(FedConvention convention) {
        this.convention = convention;
        dispatcher = ReflectUtil.createMethodDispatcher(RelNode.class, this, "replace",
                RelNode.class);
    }

    public RelNode replace(FedTableScan node) {
        return node;
    }

    public RelNode replace(Project rel) {
        return new FedRelRules.FedRelProject(
                rel.getCluster(),
                rel.getTraitSet().replace(convention),
                rel.getInput(),
                rel.getProjects(),
                rel.getRowType());
    }

    public RelNode replace(Filter rel) {
        return new FedRelRules.FedRelFilter(
                rel.getCluster(),
                rel.getTraitSet().replace(convention),
                rel.getInput(),
                rel.getCondition());
    }

    public RelNode replace(Join rel) {
        try {
            return new FedRelRules.FedRelJoin(
                    rel.getCluster(),
                    rel.getTraitSet().replace(convention),
                    rel.getInput(0),
                    rel.getInput(1),
                    rel.getCondition(),
                    rel.getVariablesSet(),
                    rel.getJoinType());
        } catch (InvalidRelException e) {
            e.printStackTrace();
        }
        return null;
    }

    public RelNode replace(Aggregate rel) {
        try {
            return new FedRelRules.FedRelAggregate(
                    rel.getCluster(),
                    rel.getTraitSet().replace(convention),
                    rel.getInput(),
                    rel.getGroupSet(),
                    rel.getGroupSets(),
                    rel.getAggCallList());
        } catch (InvalidRelException e) {
            e.printStackTrace();
        }
        return null;
    }

    public RelNode replace(Sort rel) {
        return new FedRelRules.FedRelSort(
                rel.getCluster(),
                rel.getTraitSet().replace(convention),
                rel.getInput(),
                rel.getCollation(),
                rel.offset,
                rel.fetch);
    }
}
