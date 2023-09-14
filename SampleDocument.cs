using Newtonsoft.Json;

[Serializable]
public class Document
{
    [JsonProperty(PropertyName = "id")]
    public string Id { get; set; }

    public string PartitionKey { get; set; }
}