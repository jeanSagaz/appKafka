using System.Text.Json.Serialization;

namespace Business.Models
{
    public class Value
    {
        [JsonPropertyName("input")]
        public string? Input { get; set; }
    }
}
