using System;
using System.Security.Cryptography;
using System.Text;

namespace NServiceBus.AzureFunctions.ServiceBus
{
    static class DeterministicGuid
    {
        public static Guid Create(string data)
        {
            // use MD5 hash to get a 16-byte hash of the string
            using (var provider = new MD5CryptoServiceProvider())
            {
                var inputBytes = Encoding.Default.GetBytes(data);
                var hashBytes = provider.ComputeHash(inputBytes);
                // generate a guid from the hash:
                return new Guid(hashBytes);
            }
        }
    }
}