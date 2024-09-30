package ai.dataprep.accio;

import java.util.HashMap;
import java.util.List;

public class FederatedDataSource {
    public boolean isLocal;
    public boolean isManualSchema;
    public String url;
    public String driver;
    public String username;
    public String password;
    public HashMap<String, List<String>> schemaInfo = new HashMap<>();

    public FederatedDataSource(Boolean isLocal, String url, String driver, String username, String password) {
        this.isManualSchema = false;
        this.isLocal = isLocal;
        this.url = url;
        this.driver = driver;
        this.username = username;
        this.password = password;
    }

    public FederatedDataSource(Boolean isLocal, HashMap<String, List<String>> schemaInfo) {
        this.isManualSchema = true;
        this.isLocal = isLocal;
        this.schemaInfo = schemaInfo;
    }
}
