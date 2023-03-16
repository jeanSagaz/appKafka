using System.Text.Json.Serialization;

namespace Producer.Worker.Models
{
    public class ExecuteAnythingRequest
    {
        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;
    }
}
