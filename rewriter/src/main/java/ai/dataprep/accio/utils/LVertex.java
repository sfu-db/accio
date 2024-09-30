package ai.dataprep.accio.utils;

import ai.dataprep.accio.plan.FedConvention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Arrays;
import java.util.Comparator;

// Logical Vertex
public class LVertex implements Comparator<LVertex> {
    public FedConvention convention;
    public Double rowCount;
    public RelNode rel;
    public ImmutableBitSet factors;
    public ImmutableBitSet usedFilters;
    public ImmutableBitSet unusedFilters;
    public int [] factorOffets;
    public int nFields;
    public LVertex left;
    public LVertex right;
    public boolean isJoin;
    public Double cost;
    public double [] fieldDomain;

    public LVertex(FedConvention convention, RelNode rel, ImmutableBitSet factors, ImmutableBitSet usedFilters, ImmutableBitSet unusedFilters, int [] factorOffets, int nFields, double [] fieldDomain) {
        this.convention = convention;
        this.rel = rel;
        this.factors = factors;
        this.usedFilters = usedFilters;
        this.unusedFilters = unusedFilters;
        this.factorOffets = factorOffets;
        this.nFields = nFields;
        this.rowCount = null;
        this.isJoin = false;
        this.left = null;
        this.right = null;
        this.cost = Double.MAX_VALUE;
        this.fieldDomain = fieldDomain;
    }

    public void setChildren(LVertex left, LVertex right) {
        this.isJoin = true;
        this.left = left;
        this.right = right;
    }

    @Override public String toString() {
        return "[" + convention.toString() + " | " + rel
                + "] (card=" + rowCount + "), nFields: " + nFields
                + ", unusedFilters: " + unusedFilters
                + ", factorOffsets: " + Arrays.toString(factorOffets)
                + ", fieldDomains: " + Arrays.toString(fieldDomain)
                + recursive();
    }

    protected String recursive() {
        String s = "";
        if (this.isJoin) {
            s = "\n" + this.factors.toString() + "[" + this.rowCount + "," + (this.cost - this.left.cost - this.right.cost) + "]: " + this.left.factors.toString() + " X " + this.right.factors.toString();
            s += this.left.recursive();
            s += this.right.recursive();
        }
        return s;
    }

    @Override
    public int compare(LVertex o1, LVertex o2) {
        return o1.cost.compareTo(o2.cost);
    }
}
