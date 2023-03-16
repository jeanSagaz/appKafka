using System.Text.Json.Serialization;

namespace Business.Models
{
    public class ExecuteAnythingRequest
    {
        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;
    }
}
