package ai.dataprep.accio.utils;

import org.apache.calcite.rel.rules.LoptMultiJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

public class VariableUpdater extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final int[] fieldFactorMap;
    private final int[] fieldOffsetMap;
    private int[] newFactorOffsets;

    public VariableUpdater(RexBuilder rexBuilder, LoptMultiJoin multiJoin) {
        this.rexBuilder = rexBuilder;
        this.fieldFactorMap = new int[multiJoin.getNumTotalFields()];
        this.fieldOffsetMap = new int[multiJoin.getNumTotalFields()];
        int idx = 0;
        for (int i = 0; i < multiJoin.getNumJoinFactors(); ++i) {
            for (int j = 0; j < multiJoin.getNumFieldsInJoinFactor(i); ++j) {
                fieldFactorMap[idx] = i;
                fieldOffsetMap[idx] = j;
                ++idx;
            }
        }
        assert idx == multiJoin.getNumTotalFields();
        this.newFactorOffsets = null;
    }

    public void setNewFactorOffsets(int[] newFactorOffsets) {
        this.newFactorOffsets = newFactorOffsets;
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
        assert this.newFactorOffsets != null;

        int index = inputRef.getIndex();
        int inputFactor = this.fieldFactorMap[index];
        int newIndex = this.newFactorOffsets[inputFactor] + this.fieldOffsetMap[index];
        return rexBuilder.makeInputRef(inputRef.getType(), newIndex);
    }
}
