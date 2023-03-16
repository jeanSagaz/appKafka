using Confluent.Kafka;
using System.IO.Compression;
using System.Text.Json;

namespace Adapters.Serialization
{
    public class CustomDeserializer<T> : IDeserializer<T>
    {
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull)
                return default;

            //return JsonSerializer.Deserialize<T>(data);

            using var memoryStream = new MemoryStream(data.ToArray());
            using var zip = new GZipStream(memoryStream, CompressionMode.Decompress, true);

            return JsonSerializer.Deserialize<T>(zip);
        }
    }
}
