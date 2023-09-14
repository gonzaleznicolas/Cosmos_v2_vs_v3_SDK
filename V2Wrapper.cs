using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.Documents.Client;

public class V2Wrapper
{
    private DocumentClient documentClient;

    public V2Wrapper(string accountEndpoint, string accountKey)
    {
        documentClient = new DocumentClient(
            new Uri(accountEndpoint),
            accountKey,
            new ConnectionPolicy
            {
                ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
                RequestTimeout = new TimeSpan(1, 0, 0),
                MaxConnectionLimit = 1000,
                RetryOptions = new RetryOptions
                {
                    MaxRetryAttemptsOnThrottledRequests = 10,
                    MaxRetryWaitTimeInSeconds = 60
                }
            }
        );
    }

    public async Task UpsertDocuments(List<Document> documents, string databaseName, string containerName)
    {
        var documentCollectionUri = UriFactory.CreateDocumentCollectionUri(databaseName, containerName);
        var documentCollection = await documentClient.ReadDocumentCollectionAsync(documentCollectionUri);
        var bulkExecutor = new BulkExecutor(documentClient, documentCollection.Resource);
        await bulkExecutor.InitializeAsync();

        var importTask = bulkExecutor.BulkImportAsync(documents, true);
        await importTask;
    }
}