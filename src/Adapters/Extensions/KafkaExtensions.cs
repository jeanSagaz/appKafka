using Confluent.Kafka;
using System.Text;

namespace Adapters.Extensions
{
    public static class KafkaExtensions
    {
        public static Dictionary<string, string> HeaderToDictionary(this Headers headers)
        {
            var dictionary = new Dictionary<string, string>();

            if (headers is not null && headers.Count > 0)
            {
                foreach (var header in headers)
                {
                    dictionary[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes());
                }
            }

            return dictionary;
        }

        public static Headers DictionaryToHeader(this Dictionary<string, string> dictionary)
        {
            var headers = new Headers();

            if (dictionary is not null && dictionary.Count > 0)
            {
                foreach (var property in dictionary)
                {
                    var header = new Header(
                        property.Key,
                        Encoding.UTF8.GetBytes(property.Value));

                    headers.Add(header);
                }
            }

            return headers;
        }
    }
}
