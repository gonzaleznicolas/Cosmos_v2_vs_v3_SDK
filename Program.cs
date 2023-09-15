using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;

public class Program
{
    public static async Task Main(string[] args)
    {
        await Task.Delay(0);
        
        var databaseName = "Database_v2v3";
        var containerName = "Container_v2v3";
        var accountEndpoint = "https://localhost:8081/";
        var accountKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        var numPartitionKeys = 7500;
        var docsPerPartitionKey = 10;
        var numDocuments = numPartitionKeys * docsPerPartitionKey;

        var v2Wrapper = new V2Wrapper(accountEndpoint, accountKey);
        var v3Wrapper = new V3Wrapper(accountEndpoint, accountKey);

        Console.WriteLine($"Creating database {databaseName} and container {containerName}...\n");
        await v3Wrapper.CreateDatabaseAndContainer(databaseName, containerName);

        Console.WriteLine($"Generating {numDocuments} documents...\n");
        var documents = GenerateSampleDocs(numPartitionKeys, docsPerPartitionKey);

        Console.WriteLine($"Performing an initial INSERTION of the {numDocuments} documents...\n");
        await v2Wrapper.UpsertDocuments(documents, databaseName, containerName);

        Console.WriteLine($"Compare how quickly the V2 SDK vs the V3 SDK can flush {numDocuments} documents,");
        Console.WriteLine($"and see if the V3 SDK throws a TooManyRequests Exception:\n");
        Stopwatch stopwatch;

        Console.WriteLine($"Starting V2 SDK...");
        stopwatch = Stopwatch.StartNew();
        await v2Wrapper.UpsertDocuments(documents, databaseName, containerName);
        stopwatch.Stop();
        Console.WriteLine($"V2 SDK finished after {stopwatch.ElapsedMilliseconds/1000} seconds.\n");

        Console.WriteLine($"Starting V3 SDK...");
        var didOrDidNot = "did not";
        stopwatch = Stopwatch.StartNew();
        try
        {
            await v3Wrapper.UpsertDocuments(documents, databaseName, containerName);
        }
        catch (CosmosException ex)
        {
            if (ex.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
            {
                didOrDidNot = "DID";
            }
        }
        stopwatch.Stop();
        Console.WriteLine($"V3 SDK took {stopwatch.ElapsedMilliseconds/1000} seconds and { didOrDidNot } throw a TooManyRequests Exception.\n");
    }

    public static List<Document> GenerateSampleDocs(int numPartitionKeys, int docsPerPartitionKey)
    {
        var documentList = new List<Document>();
        for (int partitionKey = 0; partitionKey < numPartitionKeys; partitionKey++)
        {
            for (int docNum = 0; docNum < docsPerPartitionKey; docNum++)
            {
                var newDoc = new Document
                {
                    Id = $"{partitionKey}_{docNum}",
                    PartitionKey = partitionKey.ToString()
                };

                documentList.Add(newDoc);
            }
        }

        return documentList;
    }
}
