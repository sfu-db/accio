package ai.dataprep.accio.stats;

public class FactorGroupStatsInfo {
    public double rowCount;
    public double[] fieldDomain; // recording the domain size of join keys

    public FactorGroupStatsInfo(double rowCount, double[] fieldDomain) {
        this.rowCount = rowCount;
        this.fieldDomain = fieldDomain;
    }
}
