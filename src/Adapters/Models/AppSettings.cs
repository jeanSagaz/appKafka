namespace Adapters.Models
{
    public class AppSettings
    {
        public KafkaOptions KafkaConfigurations { get; set; }
    }

    public class KafkaOptions
    {
        public string Host { get; set; } = string.Empty;
    }
}
