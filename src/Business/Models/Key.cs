using System.Text.Json.Serialization;

namespace Business.Models
{
    public class Key
    {
        [JsonPropertyName("id")]
        public string? Id { get; set; }
    }
}
