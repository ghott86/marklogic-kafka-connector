package kafka.connect.marklogic;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.document.DocumentManager;
import kafka.connect.marklogic.sink.MarkLogicSinkConfig;

import java.util.Map;
import java.util.UUID;

public abstract class MarkLogicWriter implements Writer {

    protected final DatabaseClient client;
    DocumentManager manager;

    public MarkLogicWriter(final Map<String, String> config){
        client = DatabaseClientFactory.newClient(config.get(MarkLogicSinkConfig.CONNECTION_HOST),
            Integer.valueOf(config.get(MarkLogicSinkConfig.CONNECTION_PORT)),
            new DatabaseClientFactory.DigestAuthContext(config.get(MarkLogicSinkConfig.CONNECTION_USER),
                config.get(MarkLogicSinkConfig.CONNECTION_PASSWORD)));
        manager = client.newJSONDocumentManager();
    }

    protected String url(){
        return UUID.randomUUID().toString();
    }
}
