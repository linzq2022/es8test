package com.qianshan;
import co.elastic.clients.elasticsearch.*;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.field_usage_stats.UsageStatsIndex;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.*;
import org.apache.http.client.*;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.*;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.*;
import org.elasticsearch.client.*;
import javax.net.ssl.SSLContext;
import java.io.InputStream;
import java.nio.file.*;
import java.security.KeyStore;
import java.security.cert.*;
import java.util.*;

/**
 * Hello world!
 *
 */
public class ESClient {
    private static ElasticsearchClient client;
    private static ElasticsearchAsyncClient asyncClient;
    private static ElasticsearchTransport transport;

    private static final String INDEX_ST = "estest";

    public static void main(String[] args) throws Exception {
        // 初始化ES服务器的连接
        initESConnection();

        // 操作索引
//        operationIndex();
//        operationIndexLambda();

        // 操作文档
//        operationDocument();
//        operationDocumentLambda();

        // 查询文档
//        queryDocument();
//          queryDocumentLambda();

        // 异步操作
        setAsyncClientOpreation();

    }


    private static void setAsyncClientOpreation() throws Exception{
        asyncClient.indices().create(
                req ->req.index("newindex")
        ).thenApply(
                resp ->resp.acknowledged()
        ).whenComplete(
                (resp, error) ->{
                    System.out.println("回调方法");
                    if (resp){
                        System.out.println(resp);
                    } else {
                        error.printStackTrace();
                    }
                }
        );
        System.out.println("主线程代码。。。。");
    }


    private static void queryDocumentLambda() throws Exception{
        System.out.println(client.search(
                 req -> {
                     req.query(
                             q -> q.match(
                                     m -> m.field("name").query("zhangsan")
                             )
                     );
                     return req;
                 },
                 Object.class
         ).hits());

        transport.close();
    }



    private static void queryDocument() throws Exception{

        MatchQuery matchQuery = new MatchQuery.Builder().field("age").query(30).build();

        Query query = new Query.Builder().match(matchQuery).build();

        SearchRequest searchRequest = new SearchRequest.Builder().query(query).build();

        SearchResponse<Object> search = client.search(searchRequest, Object.class);
        System.out.println(search);


        transport.close();

    }


    private static void operationDocumentLambda() throws Exception {
        User user = new User();
        user.setId(4001);
        user.setName("张三");
        user.setAge(30);

        System.out.println(client.create(
                req -> req.index(INDEX_ST)
                        .id("4001")
                        .document(new User(4001, "zhangsan", 30))
        ).result());

        List<User> users = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            users.add(new User(3000 + i, "lisi" + i, 20 + i));
        }

        // 批量添加数据
        client.bulk(
                req -> {
                    users.forEach(
                            u -> {
                                req.operations(
                                        b -> b.create(
                                                d -> d.index(INDEX_ST).id(u.getId().toString()).document(u)
                                        )
                                );
                            }
                    );
                    return req;
                }

        );

        // 文档的删除
        client.delete(
                req -> req.index(INDEX_ST).id("3001")
        );

        transport.close();

    }



    private static void operationDocument() throws Exception{

        User user = new User();
        user.setId(1001);
        user.setName("张三");
        user.setAge(30);

        CreateRequest<User> createRequest = new CreateRequest.Builder<User>()
                .index(INDEX_ST)
                .id("1001")
                .document(user)
                .build();


        // 增加文档
//        final CreateResponse createResponse = client.create(createRequest);
//        System.out.println("文档创建的响应对象" + createResponse);

        // 批量添加文档数据
        List<BulkOperation> opts = new ArrayList<BulkOperation>();
        for (int i = 1; i < 10; i++) {

            CreateOperation<User> optObj = new CreateOperation.Builder<User>()
                    .index(INDEX_ST)
                    .id("200" + i)
                    .document(new User(2000 + i, "zhangsan" + i, 30 + i))
                    .build()
                    ;

            BulkOperation opt = new BulkOperation.Builder().create(optObj).build();
            opts.add(opt);
        }


        BulkRequest bulkRequest = new BulkRequest.Builder()
                .operations(opts)
                .build();
//        final BulkResponse bulk = client.bulk(bulkRequest);
//        System.out.println("批量新增数据的响应：" + bulk);


        // 文档的删除
        DeleteRequest deleteRequest = new DeleteRequest.Builder()
                .index(INDEX_ST)
                .id("2001")
                .build();

        DeleteResponse deleteResponse = client.delete(deleteRequest);
        System.out.println("索引删除的响应结果：" + deleteResponse);


        transport.close();
    }


    private static void operationIndexLambda() throws Exception{

        // 获取索引客户端对象
        final ElasticsearchIndicesClient indices = client.indices();

        final boolean flg = indices.exists(req -> req.index(INDEX_ST)).value();
        if (flg){
            System.out.println("索引" + INDEX_ST + "已经存在");
        } else {
            final CreateIndexResponse createIndexResponse = indices.create(req -> req.index(INDEX_ST));
            System.out.println("创建索引的响应对象 = " + createIndexResponse);
        }

        final IndexState estest = indices.get(req -> req.index(INDEX_ST)).get(INDEX_ST);

//        System.out.println(indices.delete(req ->req.index(INDEX_ST)).acknowledged());


        transport.close();



    }




    private static  void operationIndex() throws Exception {
        // 获取索引客户端对象
        final ElasticsearchIndicesClient indices = client.indices();

        
        // 判断索引是否存在
        ExistsRequest existsRequest = new ExistsRequest.Builder().index(INDEX_ST).build();
        final boolean flg = indices.exists(existsRequest).value();
        if (flg){
            System.out.println("索引" + INDEX_ST + "已经存在");
        } else {
            // 创建索引
            // 需要采用构建方式来构建对象，ESAPI的对象基本上都是采用这种方式
            CreateIndexRequest request = new CreateIndexRequest.Builder().index(INDEX_ST).build();
            final CreateIndexResponse createIndexResponse = indices.create(request);
            System.out.println("创建索引的响应对象 = " + createIndexResponse);
        }
        // 查询索引
        GetIndexRequest getIndexRequest = new GetIndexRequest.Builder().index(INDEX_ST).build();
        GetIndexResponse getIndexResponse = indices.get(getIndexRequest);
//        final IndexState estest = getIndexResponse.get("estest");
        System.out.println("查询的响应结果：" + getIndexResponse);

        // 删除索引
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest.Builder().index(INDEX_ST).build();
        DeleteIndexResponse deleteIndexResponse = indices.delete(deleteIndexRequest);
        System.out.println("索引删除的响应结果：" + deleteIndexResponse.acknowledged());

        transport.close();

    }




    private static void initESConnection() throws Exception{
        // 获取客户端对象
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "_OL5Kq-M*NmLTxEKNE=D"));
        Path caCertificatePath = Paths.get("certs/es-api-ca.crt");
        CertificateFactory factory =
                CertificateFactory.getInstance("X.509");
        Certificate trustedCa;
        try (InputStream is = Files.newInputStream(caCertificatePath)) {
            trustedCa = factory.generateCertificate(is);
        }
        KeyStore trustStore = KeyStore.getInstance("pkcs12");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", trustedCa);
        SSLContextBuilder sslContextBuilder = SSLContexts.custom()
                .loadTrustMaterial(trustStore, null);
        final SSLContext sslContext = sslContextBuilder.build();
        RestClientBuilder builder = RestClient.builder(
                        new HttpHost("es-node-1", 9200, "https"))
                .setHttpClientConfigCallback(new
                                                     RestClientBuilder.HttpClientConfigCallback() {
                                                         @Override
                                                         public HttpAsyncClientBuilder customizeHttpClient(
                                                                 HttpAsyncClientBuilder httpClientBuilder) {
                                                             return httpClientBuilder.setSSLContext(sslContext)
                                                                     .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                                                                     .setDefaultCredentialsProvider(credentialsProvider);
                                                         }
                                                     });
        RestClient restClient = builder.build();
        transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());
        // 同步客户端对象
        client = new ElasticsearchClient(transport);

        // 异步客户端对象
        asyncClient = new ElasticsearchAsyncClient(transport);

//        transport.close();


    }


}
