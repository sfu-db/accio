package ai.dataprep.accio;

import ai.dataprep.accio.cost.FedRelMdPercentageOriginalRows;
import ai.dataprep.accio.stats.FedRelMdRowCount;
import ai.dataprep.accio.utils.FedRelMdColumnOrigins;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.*;

public class FedRelMetadataProvider extends ChainedRelMetadataProvider {
    public static final FedRelMetadataProvider INSTANCE =
            new FedRelMetadataProvider();

    protected FedRelMetadataProvider() {
        super(
                ImmutableList.of(
                        FedRelMdPercentageOriginalRows.SOURCE,
                        FedRelMdColumnOrigins.SOURCE,
                        RelMdExpressionLineage.SOURCE,
                        RelMdTableReferences.SOURCE,
                        RelMdNodeTypes.SOURCE,
                        FedRelMdRowCount.SOURCE, // locally defined
                        RelMdMaxRowCount.SOURCE,
                        RelMdMinRowCount.SOURCE,
                        RelMdUniqueKeys.SOURCE,
                        RelMdColumnUniqueness.SOURCE,
                        RelMdPopulationSize.SOURCE,
                        RelMdSize.SOURCE,
                        RelMdParallelism.SOURCE,
                        RelMdDistribution.SOURCE,
                        RelMdLowerBoundCost.SOURCE,
                        RelMdMemory.SOURCE,
                        RelMdDistinctRowCount.SOURCE,
                        RelMdSelectivity.SOURCE,
                        RelMdExplainVisibility.SOURCE,
                        RelMdPredicates.SOURCE,
                        RelMdAllPredicates.SOURCE,
                        RelMdCollation.SOURCE));
    }
}
