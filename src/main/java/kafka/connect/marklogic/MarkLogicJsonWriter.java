package kafka.connect.marklogic;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.JacksonHandle;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class MarkLogicJsonWriter extends MarkLogicWriter implements Writer{
    
    private static final Logger logger = LoggerFactory.getLogger(MarkLogicJsonWriter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String URL = "url";

    public MarkLogicJsonWriter(final Map<String, String> config){
        super(config);
        manager = (JSONDocumentManager) client.newJSONDocumentManager();
    }
    
    public void write(final Collection<SinkRecord> records){
        records.forEach(r -> {
            logger.debug("received value {}, and collection {}", r.value(), r.topic());
            final Map<?, ?> v = new LinkedHashMap<>((Map<?,?>) r.value());
            try {
                manager.write(url(v), handle(v));
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        });
    }
    
    protected String url(final Map<?, ?> valueMap ){
        return valueMap.get(URL) == null ? super.url() : valueMap.remove(URL).toString();
    }

    protected JacksonHandle handle(final Map<?, ?> valueMap ){
        return new JacksonHandle(MAPPER.convertValue(valueMap, JsonNode.class));
    }
}
