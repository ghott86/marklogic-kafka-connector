package kafka.connect.marklogic;

import com.marklogic.client.io.InputStreamHandle;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Phil Barber
 */

public class MarkLogicXmlWriter extends MarkLogicWriter implements Writer {
    private static final Logger logger = LoggerFactory.getLogger(MarkLogicXmlWriter.class);

    public MarkLogicXmlWriter(final Map<String, String> config) {
        super(config);
        manager = client.newXMLDocumentManager();
    }
    
    public void write(final Collection<SinkRecord> records) {
        records.forEach(r -> {
            logger.info("received value {}, and collection {}", r.value(), r.topic());
            try {
                manager.write(url(), handle((String) r.value()));
                logger.info("Wrote record");
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        });
    }

    protected InputStreamHandle handle(String value) {
        return new InputStreamHandle(new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8)));
    }

}