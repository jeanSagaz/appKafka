using Confluent.Kafka;
using System.IO.Compression;
using System.Text;
using System.Text.Json;

namespace Adapters.Serialization
{
    public class CustomSerializer<T> : ISerializer<T>
    {
        public byte[] Serialize(T data, SerializationContext context)
        {
            //return Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data, typeof(T)));
            //return Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data, new JsonSerializerOptions { WriteIndented = true }));

            var bytes = JsonSerializer.SerializeToUtf8Bytes(data);

            using var memoryStream = new MemoryStream();
            using var zipStream = new GZipStream(memoryStream, CompressionMode.Compress, true);
            zipStream.Write(bytes, 0, bytes.Length);
            zipStream.Close();
            var buffer = memoryStream.ToArray();

            return buffer;
        }
    }
}
