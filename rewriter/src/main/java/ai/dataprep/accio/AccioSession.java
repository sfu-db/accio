package ai.dataprep.accio;

import org.apache.calcite.schema.SchemaPlus;


public class AccioSession {
    public SchemaPlus rootSchema;
    public AccioSession(SchemaPlus rootSchema) {
        this.rootSchema = rootSchema;
    }
}
