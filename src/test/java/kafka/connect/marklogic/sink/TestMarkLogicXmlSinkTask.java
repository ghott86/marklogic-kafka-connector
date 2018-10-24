package kafka.connect.marklogic.sink;

import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.MatchDocumentSummary;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawQueryByExampleDefinition;
import kafka.connect.marklogic.AbstractTest;
import kafka.connect.marklogic.MarkLogicXmlWriter;
import org.apache.http.client.ClientProtocolException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import static org.junit.Assert.assertEquals;


/**
 * 
 * @author Sanju Thomas
 *
 */
public class TestMarkLogicXmlSinkTask extends AbstractTest{
	
	private MarkLogicSinkTask markLogicSinkTask;
	
	@Before
	public void setup(){
	    super.setup();
		markLogicSinkTask = new MarkLogicSinkTask();
        Map<String, String> conf = super.conf;
        conf.put(MarkLogicSinkConfig.WRITER_IMPL, MarkLogicXmlWriter.class.getCanonicalName());
        markLogicSinkTask.start(super.conf);
	}
	
	
	@Test
	public void shouldPut() throws ClientProtocolException, IOException, URISyntaxException{
		
		List<SinkRecord> documents = new ArrayList<SinkRecord>();
		final String xml ="<foo>" + UUID.randomUUID().toString() + "</foo>";

		documents.add(new SinkRecord("trades", 1, null, null, null,  xml, 0));
		markLogicSinkTask.put(documents);

        StringHandle handle = new StringHandle(
            "<q:qbe xmlns:q=\"http://marklogic.com/appservices/querybyexample\">\n" +
                "  <q:query>\n" +
                "    " + xml + "\n" +
                "  </q:query>\n" +
                "</q:qbe>"
        );
        QueryManager queryMgr = client.newQueryManager();
        RawQueryByExampleDefinition query = queryMgr.newRawQueryByExampleDefinition(handle);
        SearchHandle searchHandle = new SearchHandle();
        queryMgr.search(query, searchHandle);

        assertEquals(1, searchHandle.getTotalResults());

        for (MatchDocumentSummary docSum: searchHandle.getMatchResults()) {
            client.newDocumentManager().delete(docSum.getUri());
        }
	}
}
