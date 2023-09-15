using System.Collections.Concurrent;
using Microsoft.Azure.Cosmos;

public class V3Wrapper
{
    private CosmosClient cosmosClient;

    public V3Wrapper(string accountEndpoint, string accountKey)
    {
        cosmosClient = new CosmosClient(
            accountEndpoint,
            accountKey, new CosmosClientOptions
            {
                ConnectionMode = Microsoft.Azure.Cosmos.ConnectionMode.Direct,
                RequestTimeout = new TimeSpan(1, 0, 0),
                GatewayModeMaxConnectionLimit = 1000,
                MaxRetryAttemptsOnRateLimitedRequests = 1000,
                MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromHours(1),
                AllowBulkExecution = true
            }
        );
    }

    public async Task CreateDatabaseAndContainer(string databaseName, string containerName)
    {
        Database database = await cosmosClient.CreateDatabaseIfNotExistsAsync(databaseName);
        ContainerProperties containerProperties = new ContainerProperties
            {
                Id = containerName,
                PartitionKeyPath = "/PartitionKey"
            };
        ThroughputProperties throughputProperties = ThroughputProperties.CreateManualThroughput(400);
        await database.CreateContainerIfNotExistsAsync(containerProperties, throughputProperties);
    }

    public async Task UpsertDocuments(List<Document> documents, string databaseName, string containerName)
    {
        var container = cosmosClient.GetContainer(databaseName, containerName);
        var tasks = new List<Task>();
        foreach (var document in documents)
        {
            tasks.Add(container.UpsertItemAsync(document, new PartitionKey(document.PartitionKey)));
        }

        await Task.WhenAll(tasks);
    }
}