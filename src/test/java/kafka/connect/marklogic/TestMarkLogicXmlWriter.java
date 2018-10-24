package kafka.connect.marklogic;

import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.MatchDocumentSummary;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawQueryByExampleDefinition;
import kafka.connect.marklogic.beans.Account;
import kafka.connect.marklogic.beans.Client;
import kafka.connect.marklogic.beans.QuoteRequest;
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
public class TestMarkLogicXmlWriter extends AbstractTest{
    
    private Writer writer;
    
    @Before
    public void setup(){
        super.setup();
        writer = new MarkLogicXmlWriter(super.conf);
    }

    @Test
    public void shouldWrite() throws ClientProtocolException, IOException, URISyntaxException{
        
        final List<SinkRecord> documents = new ArrayList<SinkRecord>();
        final String xmlDocument1 ="<foo>" + UUID.randomUUID().toString() + "</foo>";

        documents.add(new SinkRecord("topic", 1, null, null, null, xmlDocument1, 0));
        writer.write(documents);

        StringHandle handle = new StringHandle(
            "<q:qbe xmlns:q=\"http://marklogic.com/appservices/querybyexample\">\n" +
                "  <q:query>\n" +
                "    " + xmlDocument1 + "\n" +
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
