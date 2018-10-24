package kafka.connect.marklogic;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.connect.marklogic.beans.QuoteRequest;
import kafka.connect.marklogic.sink.MarkLogicSinkConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.DigestAuthContext;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.JacksonHandle;

/**
 * 
 * @author Sanju Thomas
 *
 */
public abstract class AbstractTest {    
    protected static final String propFileName = "marklogic-json-sink.properties";
    protected static final ObjectMapper MAPPER = new ObjectMapper();
    protected final Map<String, String> conf = new HashMap<>();
    protected JSONDocumentManager manager;
    protected DatabaseClient client;

    public void setup() throws IOException {
        Properties props = getProps();

        conf.put(MarkLogicSinkConfig.CONNECTION_HOST, props.getProperty("ml.connection.host"));
        conf.put(MarkLogicSinkConfig.CONNECTION_PORT, props.getProperty("ml.connection.port"));
        conf.put(MarkLogicSinkConfig.CONNECTION_USER, props.getProperty("ml.connection.user"));
        conf.put(MarkLogicSinkConfig.CONNECTION_PASSWORD, props.getProperty("ml.connection.password"));
        conf.put(MarkLogicSinkConfig.BATCH_SIZE, props.getProperty("ml.batch.size"));
        conf.put(MarkLogicSinkConfig.WRITER_IMPL, props.getProperty("ml.writer.impl"));
        conf.put(MarkLogicSinkConfig.RETRY_BACKOFF_MS, props.getProperty("retry.backoff.ms"));
        conf.put(MarkLogicSinkConfig.MAX_RETRIES, props.getProperty("max.retries"));
        conf.put("topics", "trades");
        
        client = DatabaseClientFactory.newClient(
            props.getProperty("ml.connection.host"), 
            Integer.parseInt(props.getProperty("ml.connection.port")), 
            new DigestAuthContext(
                props.getProperty("ml.connection.user"), 
                props.getProperty("ml.connection.password")
            )
        );
        manager = client.newJSONDocumentManager();
    }

    public QuoteRequest find(String url){
        final DocumentPage documentPage = manager.read(url);

        if (documentPage.hasNext()) {
            JacksonHandle handle = new JacksonHandle();
            handle = documentPage.nextContent(handle);
            return MAPPER.convertValue(handle.get(), QuoteRequest.class);
        }

        return null;
    }
    
    public void delete(String url){
        manager.delete(url);
    }
 
    public Properties getProps() throws IOException {
        Properties props = new Properties();
        InputStream inputStream = new FileInputStream(new File("./config/" + propFileName));
 
        if (inputStream != null) {
            props.load(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + propFileName + "' not found");
        }

        inputStream.close();
        return props;
    }

}
