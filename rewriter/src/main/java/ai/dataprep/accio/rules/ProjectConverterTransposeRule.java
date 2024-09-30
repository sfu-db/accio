package ai.dataprep.accio.rules;

import ai.dataprep.accio.plan.FedRelRules;
import ai.dataprep.accio.plan.RemoteToLocalConverter;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.TransformationRule;
import org.immutables.value.Value;

@Value.Enclosing
public class ProjectConverterTransposeRule extends RelRule<ProjectConverterTransposeRule.Config> implements TransformationRule {

    protected ProjectConverterTransposeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Project project = call.rel(0);
        final RemoteToLocalConverter converter = call.rel(1);
        final RelNode input = converter.getInput();

        final RelNode newProject = new FedRelRules.FedRelProject(
                input.getCluster(),
                input.getTraitSet(),
                input,
                project.getProjects(),
                project.getRowType());
        final RemoteToLocalConverter newConverter = new RemoteToLocalConverter(converter.getCluster(), converter.getTraitSet(), newProject, converter.getPartitionScheme(), converter.getRowCount());
        call.transformTo(newConverter);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        ProjectConverterTransposeRule.Config DEFAULT = ImmutableProjectConverterTransposeRule.Config.builder().build().withOperandFor(Project.class, RemoteToLocalConverter.class);

        @Override default ProjectConverterTransposeRule toRule() {
            return new ProjectConverterTransposeRule(this);
        }

        default ProjectConverterTransposeRule.Config withOperandFor(Class<? extends Project> projectClass,
                                                                 Class<? extends ConverterImpl> converterClass) {
            return withOperandSupplier(b0 ->
                    b0.operand(projectClass).oneInput(b1 ->
                            b1.operand(converterClass).anyInputs()))
                    .as(ProjectConverterTransposeRule.Config.class);
        }
    }
}
