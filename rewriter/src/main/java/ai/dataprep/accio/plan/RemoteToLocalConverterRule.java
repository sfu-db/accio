package ai.dataprep.accio.plan;

import ai.dataprep.accio.partition.PlanPartitioner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RemoteToLocalConverterRule extends ConverterRule {

    protected RemoteToLocalConverterRule(Config config) {
        super(config);
    }

    public static RemoteToLocalConverterRule create(FedConvention out) {
        return Config.INSTANCE
                .withConversion(RelNode.class, out, FedConvention.LOCAL_INSTANCE,
                        "RemoteToLocalConverterRule")
                .withRuleFactory(RemoteToLocalConverterRule::new)
                .toRule(RemoteToLocalConverterRule.class);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        double dRows = rel.getCluster().getMetadataQuery().getRowCount(rel);
        FedConvention convention = (FedConvention) (rel.getConvention());
        PlanPartitioner.PartitionScheme partitionScheme = convention.planPartitioner.getPartitionScheme(convention, rel, dRows);
        RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutTrait());
        return new RemoteToLocalConverter(rel.getCluster(), newTraitSet, rel, partitionScheme, (long) dRows);
    }
}
