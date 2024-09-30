package ai.dataprep.accio.stats;

import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.utils.SingleSourcePushdownChecker;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.*;
import org.checkerframework.checker.nullness.qual.Nullable;

public class FedRelMdRowCount implements MetadataHandler<BuiltInMetadata.RowCount> {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    new FedRelMdRowCount(), BuiltInMetadata.RowCount.Handler.class);

    public static FedConvention getBestConvention(RelNode rel) {
        SingleSourcePushdownChecker pushdownChecker = new SingleSourcePushdownChecker(true);
        pushdownChecker.go(rel);
        // if not a query on a single source or cannot do estimation by the source, use global convention
        return pushdownChecker.convention.orElse(FedConvention.GLOBAL_INSTANCE);
    }

    public static @Nullable Double redirectToLogicalRel(RelNode rel, RelMetadataQuery mq) {
        // rowCount of rel should be the same logically (same within a RelSet)
        if (rel.getCluster().getPlanner() instanceof VolcanoPlanner) {
            VolcanoPlanner planner = (VolcanoPlanner) rel.getCluster().getPlanner();
            RelSubset subset = planner.getSubset(rel, rel.getCluster().traitSetOf(Convention.NONE));
            if (subset != null) {
                RelNode r = subset.getOriginal();
                return mq.getRowCount(r);
            }
            // the following assertion no longer valid when we construct join pushdown directly (e.g., DPsizeRewriter)
//            assert subset != null; // equivalent logical operator should be there already
        }
        return rel.estimateRowCount(mq);
    }

    //~ Methods ----------------------------------------------------------------

    public @Nullable Double getRowCount(RelNode rel, RelMetadataQuery mq) {return rel.estimateRowCount(mq);}

    public @Nullable Double getRowCount(LogicalFilter rel, RelMetadataQuery mq) {
        FedConvention convention = getBestConvention(rel);
        return convention.cardinalityEstimator.getRowCount(convention, rel, mq);
    }

    public @Nullable Double getRowCount(Filter rel, RelMetadataQuery mq) {
        return redirectToLogicalRel(rel, mq);
    }

    public @Nullable Double getRowCount(LogicalJoin rel, RelMetadataQuery mq) {
        FedConvention convention = getBestConvention(rel);
        return convention.cardinalityEstimator.getRowCount(convention, rel, mq);
    }

    public @Nullable Double getRowCount(Join rel, RelMetadataQuery mq) {
        return redirectToLogicalRel(rel, mq);
    }

    public @Nullable Double getRowCount(LogicalAggregate rel, RelMetadataQuery mq) {
        FedConvention convention = getBestConvention(rel);
        return convention.cardinalityEstimator.getRowCount(convention, rel, mq);
    }

    public @Nullable Double getRowCount(Aggregate rel, RelMetadataQuery mq) {
        return redirectToLogicalRel(rel, mq);
    }

    @Override
    public MetadataDef<BuiltInMetadata.RowCount> getDef() {
        return BuiltInMetadata.RowCount.DEF;
    }
}
