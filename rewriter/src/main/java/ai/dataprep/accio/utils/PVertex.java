package ai.dataprep.accio.utils;

import ai.dataprep.accio.partition.PlanPartitioner;
import ai.dataprep.accio.plan.FedConvention;
import ai.dataprep.accio.plan.FedRel;
import ai.dataprep.accio.plan.FedRelRules;
import ai.dataprep.accio.plan.RemoteToLocalConverter;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Arrays;

public class PVertex extends LVertex {
    public FedConvention outConvention;
    public PlanPartitioner.PartitionScheme partitionScheme;

    public PVertex(FedConvention convention, FedConvention outConvention, RelNode rel, ImmutableBitSet factors, ImmutableBitSet usedFilters, ImmutableBitSet unusedFilters, int [] factorOffets, int nFields, double [] fieldDomain) {
        super(convention, rel, factors, usedFilters, unusedFilters, factorOffets, nFields, fieldDomain);
        this.outConvention = outConvention; // outConvention != convention meaning the vertex's result is fetched to outConvention from convention
        this.partitionScheme = null;
    }

    // generate a new vertex as fetching the current vertex to local, from now on the new vertex is considered as a whole base factor
    public PVertex fetchToLocal() {
        assert this.convention != FedConvention.LOCAL_INSTANCE;
        PVertex newVertex = new PVertex(this.convention, FedConvention.LOCAL_INSTANCE, this.rel, this.factors, this.usedFilters, this.unusedFilters, this.factorOffets, this.nFields, this.fieldDomain);
        newVertex.rowCount = this.rowCount;
        // NOTE: we can do this because we only partition on single table, if not this condition should be removed
        if (!this.isJoin) {
            newVertex.partitionScheme = this.convention.planPartitioner.getPartitionScheme(this.convention, this.rel, this.rowCount);
        }
        if (newVertex.partitionScheme != null) {
            newVertex.cost = this.cost + this.convention.costEstimator.computeTransferCost(this.convention.planPartitioner.computeCostAfterPartition(this.rowCount, newVertex.partitionScheme));
        } else {
            newVertex.cost = this.cost + this.convention.costEstimator.computeTransferCost(this.rowCount);
        }
        return newVertex;
    }

    // copy the vertex information as it is conducted locally, keep the join information but replace the inputs
    public PVertex copyAsLocal(PVertex newLeft, PVertex newRight) {
        assert this.convention != FedConvention.LOCAL_INSTANCE;
        PVertex newVertex = new PVertex(FedConvention.LOCAL_INSTANCE, FedConvention.LOCAL_INSTANCE, this.rel, this.factors, this.usedFilters, this.unusedFilters, this.factorOffets, this.nFields, this.fieldDomain);
        newVertex.rowCount = this.rowCount;
//        newVertex.cost = newLeft.cost + newRight.cost + FedConvention.LOCAL_INSTANCE.costEstimator.computeInplaceJoin(newVertex.rowCount, newLeft.rowCount, newRight.rowCount);
        newVertex.isJoin = this.isJoin;
        newVertex.left = newLeft;
        newVertex.right = newRight;
        return newVertex;
    }

    @Override public String toString() {
        return "[" + convention.toString() + ">" + outConvention.toString() + " | " + factors
                + "] (cost=" + cost
                + ", card=" + rowCount + "), nFields: " + nFields
                + ", unusedFilters: " + unusedFilters
                + ", factorOffsets: " + Arrays.toString(factorOffets)
                + ", fieldDomains: " + Arrays.toString(fieldDomain)
                + recursive();
    }
}
