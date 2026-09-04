//using ProtoBuf;
//using ProtoBuf.Meta;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Linq.Expressions;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServerSharedData
{
    public class SharedUtils
    {
        //public static string GetPropertyName(string name)
        //{
        //    return name.Split(".,".ToCharArray(), StringSplitOptions.RemoveEmptyEntries)[1].Replace(")", "");
        //}

        static object _lock = new object();
        static string base_path = null;
        public static void LogError(string base_path_local, string message, Exception ex)
        {
            if (base_path == null && base_path_local != null)
            {
                lock (_lock)
                {
                    if (base_path == null)
                    {
                        base_path = base_path_local;
                    }
                }
            }

            if (base_path == null)
            {
                return;
            }
            var error_dir = Path.Combine(base_path, "logs");
            if (!Directory.Exists(error_dir))
            {
                lock (_lock)
                {
                    if (!Directory.Exists(error_dir))
                    {
                        Directory.CreateDirectory(error_dir);
                    }
                }
            }
            var now = DateTime.Now;
            var date = now.ToString("yyyy-MM-dd HH:mm:ss");
            var error = date + " " + message + " " + ex.Message + " " + ex.StackTrace + (ex.InnerException != null ? (" " + ex.InnerException.Message + " " + ex.InnerException.StackTrace) : "");
            var file_name = $"{now.Year}_{now.Month.ToString().PadLeft(2, '0')}_{now.Day.ToString().PadLeft(2, '0')}_errors.txt";
            var file_path = Path.Combine(error_dir, file_name);
            lock (_lock)
            {
                if (File.Exists(file_path))
                {
                    File.AppendAllText(file_path, error + Environment.NewLine);
                }
                else
                { 
                    File.WriteAllText(file_path, error + Environment.NewLine);
                }
            }
        }

        public static void LogInfo(string base_path_local, string message)
        {
            if (base_path == null && base_path_local != null)
            {
                lock (_lock)
                {
                    if (base_path == null)
                    {
                        base_path = base_path_local;
                    }
                }
            }

            if (base_path == null)
            {
                return;
            }

            var error_dir = Path.Combine(base_path, "logs");
            if (!Directory.Exists(error_dir))
            {
                lock (_lock)
                {
                    if (!Directory.Exists(error_dir))
                    {
                        Directory.CreateDirectory(error_dir);
                    }
                }
            }
            var now = DateTime.Now;
            var date = now.ToString("yyyy-MM-dd HH:mm:ss");
            var error = date + " " + message;
            var file_name = $"{now.Year}_{now.Month.ToString().PadLeft(2, '0')}_{now.Day.ToString().PadLeft(2, '0')}_info.txt";
            var file_path = Path.Combine(error_dir, file_name);
            lock (_lock)
            {
                if (File.Exists(file_path))
                {
                    File.AppendAllText(file_path, error + Environment.NewLine);
                }
                else
                {
                    File.WriteAllText(file_path, error + Environment.NewLine);
                }
            }
        }

        public static string GetPropertyNameFromBody(Expression keySelector)
        {
            Expression body = keySelector;

            // Handle conversions like Convert(f.PeriodId) or Convert(f.PeriodId, Nullable)
            if (body is UnaryExpression unaryExpression)
            {
                body = unaryExpression.Operand;
            }

            if (body is MemberExpression memberExpression)
            {
                return memberExpression.Member.Name;
            }

            throw new ArgumentException("Linqdb: Invalid expression format.");
        }

        public static string GetPropertyName<T, TKey>(Expression<Func<T, TKey>> keySelector)
        {
            Expression body = keySelector.Body;

            // Handle conversions like Convert(f.PeriodId) or Convert(f.PeriodId, Nullable)
            if (body is UnaryExpression unaryExpression)
            {
                body = unaryExpression.Operand;
            }

            if (body is MemberExpression memberExpression)
            {
                return memberExpression.Member.Name;
            }

            throw new ArgumentException("Linqdb: Invalid expression format.");
        }

        public static void DeleteFilesAndFoldersRecursively(string target_dir)
        {
            var path = Path.Combine(Directory.GetCurrentDirectory(), target_dir);
            if (!Directory.Exists(path))
            {
                return;
            }
            foreach (string file in Directory.GetFiles(path))
            {
                File.Delete(file);
            }

            foreach (string subDir in Directory.GetDirectories(path))
            {
                DeleteFilesAndFoldersRecursively(subDir);
            }

            Thread.Sleep(1); // This makes the difference between whether it works or not. Sleep(0) is not enough.
            Directory.Delete(path);
        }

        public static string Decompress(string input)
        {
            byte[] compressed = Convert.FromBase64String(input);
            byte[] decompressed = Decompress(compressed);
            return Encoding.UTF8.GetString(decompressed);
        }

        public static string Compress(string input)
        {
            byte[] encoded = Encoding.UTF8.GetBytes(input);
            byte[] compressed = Compress(encoded);
            return Convert.ToBase64String(compressed);
        }

        public static byte[] Decompress(byte[] input)
        {
            using (var source = new MemoryStream(input))
            {
                // read stored length
                byte[] lengthBytes = new byte[4];
                int read = source.Read(lengthBytes, 0, 4);
                if (read != 4)
                    throw new InvalidDataException("Linqdb: Not enough data to read length prefix.");

                int length = BitConverter.ToInt32(lengthBytes, 0);

                using (var decompressionStream = new GZipStream(source, CompressionMode.Decompress))
                {
                    var result = new byte[length];
                    int offset = 0;

                    while (offset < length)
                    {
                        int bytesRead = decompressionStream.Read(result, offset, length - offset);
                        if (bytesRead == 0)
                        {
                            // Compressed data ended before we got "length" bytes
                            throw new EndOfStreamException(
                                $"Linqdb: Unexpected end of decompressed data. Expected {length}, got {offset}.");
                        }

                        offset += bytesRead;
                    }

                    return result;
                }
            }
        }

        public static byte[] Compress(byte[] input)
        {
            using (var result = new MemoryStream())
            {
                var lengthBytes = BitConverter.GetBytes(input.Length);
                result.Write(lengthBytes, 0, 4);

                using (var compressionStream = new GZipStream(result,
                    CompressionMode.Compress))
                {
                    compressionStream.Write(input, 0, input.Length);
                    compressionStream.Flush();

                }
                return result.ToArray();
            }
        }



        //public static TData DeserializeFromBytes<TData>(byte[] b)
        //{
        //    using (var stream = new MemoryStream(b))
        //    {
        //        var formatter = new BinaryFormatter();
        //        stream.Seek(0, SeekOrigin.Begin);
        //        return (TData)formatter.Deserialize(stream);
        //    }
        //}

        //public static byte[] SerializeToBytes<TData>(TData settings)
        //{
        //    using (var stream = new MemoryStream())
        //    {
        //        var formatter = new BinaryFormatter();
        //        formatter.Serialize(stream, settings);
        //        stream.Flush();
        //        stream.Position = 0;
        //        return stream.ToArray();
        //    }
        //}

        public static class FloatByteConverter
        {
            // Convert float[] to byte[]
            public static byte[] FloatArrayToByteArray(float[] floats)
            {
                if (floats == null)
                {
                    return null;
                }
                byte[] bytes = new byte[floats.Length * sizeof(float)];
                Buffer.BlockCopy(floats, 0, bytes, 0, bytes.Length);
                return bytes;
            }

            // Convert byte[] to float[]
            public static float[] ByteArrayToFloatArray(byte[] bytes)
            {
                if (bytes == null)
                {
                    return null;
                }
                if (bytes.Length % sizeof(float) != 0)
                    throw new ArgumentException("Byte array length must be a multiple of 4.");

                float[] floats = new float[bytes.Length / sizeof(float)];
                Buffer.BlockCopy(bytes, 0, floats, 0, bytes.Length);
                return floats;
            }
        }

        public static class IntListSerializer
        {
            public static byte[] ToByteArray(List<int> list)
            {
                if (list == null)
                    return null;

                byte[] result = new byte[list.Count * sizeof(int)];
                Buffer.BlockCopy(list.ToArray(), 0, result, 0, result.Length);
                return result;
            }

            public static List<int> FromByteArray(byte[] data)
            {
                if (data == null)
                    return new List<int>();

                if (data.Length % sizeof(int) != 0)
                    throw new ArgumentException("Byte array length is not a multiple of 4.", nameof(data));

                int count = data.Length / sizeof(int);
                int[] arr = new int[count];
                Buffer.BlockCopy(data, 0, arr, 0, data.Length);
                return new List<int>(arr);
            }
        }
        public static class SimpleIntDoubleCodec
        {
            // Serialize: [key:int32][value:double] repeated; byte[] length must be 12 * pairCount.
            // Note: Uses BitConverter endianness (little-endian on Windows/.NET Framework).
            public static byte[] ToBytes(Dictionary<int, double> dict)
            {
                if (dict == null) throw new ArgumentNullException(nameof(dict));

                int n = dict.Count;
                byte[] data = new byte[n * 12];
                int offset = 0;

                foreach (var kv in dict)
                {
                    // int -> 4 bytes
                    var ib = BitConverter.GetBytes(kv.Key);
                    Buffer.BlockCopy(ib, 0, data, offset, 4);
                    offset += 4;

                    // double -> 8 bytes
                    var db = BitConverter.GetBytes(kv.Value);
                    Buffer.BlockCopy(db, 0, data, offset, 8);
                    offset += 8;
                }

                return data;
            }

            // Deserialize: expects data.Length % 12 == 0
            public static Dictionary<int, double> FromBytes(byte[] data)
            {
                if (data == null) throw new ArgumentNullException(nameof(data));
                if (data.Length % 12 != 0) throw new ArgumentException("Byte array length must be a multiple of 12.", nameof(data));

                int pairs = data.Length / 12;
                var dict = new Dictionary<int, double>(pairs);
                int offset = 0;

                for (int i = 0; i < pairs; i++)
                {
                    int key = BitConverter.ToInt32(data, offset);
                    offset += 4;

                    double value = BitConverter.ToDouble(data, offset);
                    offset += 8;

                    // If duplicate keys occur, last one wins; change to TryAdd if you prefer exception.
                    dict[key] = value;
                }

                return dict;
            }
        }

        public static class DecimalConversion
        {
            // Maximum scale for .NET decimal is 28-29 digits.
            // We'll consistently use 28 so that decimals like 123.045 and 123.45
            // produce different fractional integers.
            private const int FixedScale = 28;

            // We'll store each BigInteger (real, fraction) in 16 bytes in big-endian.
            // Then the total is 32 bytes per decimal.
            private const int ByteArrayLength = 16;

            /// <summary>
            /// Converts the decimal to 32 bytes in big-endian two’s-complement form:
            /// [16 bytes realPart][16 bytes fractionalPart].
            /// </summary>
            public static byte[] ToByteArray(decimal value)
            {
                // 1) Split into real and fractional
                decimal realPart = Math.Truncate(value);
                decimal fracPart = value - realPart;

                // 2) Convert each to BigInteger
                //    multiply frac by 10^FixedScale to preserve trailing zeros
                BigInteger realBigInt = new BigInteger(realPart);
                BigInteger fracBigInt = new BigInteger(fracPart * Pow10(FixedScale));

                // 3) Convert each BigInteger to fixed-length, big-endian two’s-complement
                byte[] realBytes = ToFixedBigEndianBytes(realBigInt, ByteArrayLength);
                byte[] fracBytes = ToFixedBigEndianBytes(fracBigInt, ByteArrayLength);

                // 4) Concatenate real + fractional
                return ConcatenateArrays(realBytes, fracBytes);
            }

            /// <summary>
            /// Reconstructs the decimal from a 32-byte array:
            /// [16 bytes realPart][16 bytes fractionalPart] in big-endian two’s-complement.
            /// </summary>
            public static decimal FromByteArray(byte[] bytes)
            {
                if (bytes.Length != ByteArrayLength * 2)
                {
                    throw new ArgumentException(
                        $"Byte array must be exactly {ByteArrayLength * 2} bytes long.");
                }

                // 1) Split into real (first 16 bytes) and fraction (next 16)
                byte[] realBytes = new byte[ByteArrayLength];
                byte[] fracBytes = new byte[ByteArrayLength];
                Buffer.BlockCopy(bytes, 0, realBytes, 0, ByteArrayLength);
                Buffer.BlockCopy(bytes, ByteArrayLength, fracBytes, 0, ByteArrayLength);

                // 2) Convert from big-endian to BigInteger
                BigInteger realBigInt = FromFixedBigEndianBytes(realBytes);
                BigInteger fracBigInt = FromFixedBigEndianBytes(fracBytes);

                // 3) Convert fraction back by dividing by 10^FixedScale
                decimal realPart = (decimal)realBigInt;
                decimal fractionalPart = (decimal)fracBigInt / Pow10(FixedScale);

                // 4) Combine
                return realPart + fractionalPart;
            }

            /// <summary>
            /// Computes 10^exponent as a decimal.  For exponent up to 28, this is exact.
            /// </summary>
            private static decimal Pow10(int exponent)
            {
                // Quick way to build e.g. "1000...0" with 'exponent' zeroes
                return decimal.Parse("1" + new string('0', exponent));
            }

            /// <summary>
            /// Converts a BigInteger to a fixed-length, big-endian two’s-complement byte array.
            /// Similar to BitConverter, but big-endian instead of little-endian.
            /// Ensures the output is exactly 'length' bytes.
            /// </summary>
            private static byte[] ToFixedBigEndianBytes(BigInteger value, int length)
            {
                // 1) Let .NET produce a *little-endian* two’s-complement array
                //    e.g.   raw[0] = least significant byte
                byte[] little = value.ToByteArray();

                // 2) Figure out if we need to sign-extend
                //    For negative numbers, sign extension is 0xFF; for positive, 0x00
                byte signByte = (value.Sign < 0) ? (byte)0xFF : (byte)0x00;

                // 3) We will build a big-endian array, so index 0 is the *most* significant byte
                byte[] big = new byte[length];

                // We'll fill from the *right* end of 'big' backward with the bytes from 'little'
                // and fill any remaining leading bytes with 'signByte' for extension.
                int writePos = length - 1;      // start at the rightmost big-endian index
                for (int i = 0; i < little.Length; i++)
                {
                    // as long as we haven't exhausted 'big'
                    if (writePos >= 0)
                    {
                        big[writePos] = little[i];
                        writePos--;
                    }
                    else
                    {
                        // If raw is bigger than 'length', we may be truncating; it must be only sign bytes.
                        // If there's a mismatch in sign, that implies actual data got truncated -> error.
                        if (little[i] != signByte)
                        {
                            throw new OverflowException(
                                $"BigInteger doesn't fit in {length} bytes of big-endian storage.");
                        }
                    }
                }

                // fill any remaining upper bytes with the sign byte
                while (writePos >= 0)
                {
                    big[writePos] = signByte;
                    writePos--;
                }

                return big;
            }

            /// <summary>
            /// Converts a fixed-length, big-endian two’s-complement byte array back into a BigInteger.
            /// </summary>
            private static BigInteger FromFixedBigEndianBytes(byte[] bigEndian)
            {
                // We'll convert it back to the format BigInteger expects:
                // little-endian two’s-complement. So we reverse the array, etc.
                byte signByte = (bigEndian[0] & 0x80) != 0 ? (byte)0xFF : (byte)0x00;

                // We'll create a little-endian array with possible sign extension appended to the end.
                byte[] little = new byte[bigEndian.Length + 1]; // +1 for potential sign extension
                int writePos = 0;
                // copy from the rightmost big-endian byte down to the left
                for (int i = bigEndian.Length - 1; i >= 0; i--)
                {
                    little[writePos++] = bigEndian[i];
                }

                // The last byte in 'little' can help BigInteger interpret sign
                // if the top bit of bigEndian was set (meaning negative).
                little[writePos] = signByte;

                return new BigInteger(little);
            }

            /// <summary>
            /// Concatenates arrays in order. (Real first, fraction second.)
            /// </summary>
            private static byte[] ConcatenateArrays(params byte[][] arrays)
            {
                int total = 0;
                foreach (var arr in arrays) total += arr.Length;

                byte[] result = new byte[total];
                int offset = 0;
                foreach (var arr in arrays)
                {
                    Buffer.BlockCopy(arr, 0, result, offset, arr.Length);
                    offset += arr.Length;
                }
                return result;
            }
        }

        public static int HashFnv1a(string input, int size)
        {
            const uint fnvPrime = 16777619;
            uint hash = 2166136261;

            foreach (char c in input)
            {
                hash ^= c;
                hash *= fnvPrime;
            }

            return (int)(hash % size);
        }

        public static int HashDjb2(string input, int size)
        {
            ulong hash = 5381;

            foreach (char c in input)
            {
                hash = ((hash << 5) + hash) + c; // hash * 33 + c
            }

            return (int)(hash % (ulong)size);
        }

        /// <summary>
        /// SDBM hash (often used in Unix systems for filename hashing).
        /// </summary>
        public static int HashSdbm(string input, int size)
        {
            // Use a 64‐bit accumulator to reduce overflow chances on long strings
            ulong hash = 0;

            foreach (char c in input)
            {
                // hash = c + (hash << 6) + (hash << 16) - hash;
                hash = (ulong)c + (hash << 6) + (hash << 16) - hash;
            }

            return (int)(hash % (ulong)size);
        }

        /// <summary>
        /// BKDR hash (a simple seed‐multiplicative hash).
        /// </summary>
        //public static int HashBkdr(string input, int size)
        //{
        //    // 131 is a common seed; you could also use 31, 1313, 13131, etc.
        //    const ulong seed = 131;
        //    ulong hash = 0;

        //    foreach (char c in input)
        //    {
        //        hash = hash * seed + c;
        //    }

        //    return (int)(hash % (ulong)size);
        //}

        /// <summary>
        /// Jenkins’s one‐at‐a‐time hash (a classic non‐cryptographic hash by Bob Jenkins).
        /// </summary>
        //public static int HashJenkins(string input, int size)
        //{
        //    uint hash = 0;

        //    foreach (char c in input)
        //    {
        //        hash += c;
        //        hash += (hash << 10);
        //        hash ^= (hash >> 6);
        //    }

        //    // final “mix” steps
        //    hash += (hash << 3);
        //    hash ^= (hash >> 11);
        //    hash += (hash << 15);

        //    return (int)(hash % (uint)size);
        //}

        public static void SetBit(byte[] array, long bitIndex, bool value)
        {
            if (bitIndex < 0 || array == null)
                throw new ArgumentOutOfRangeException();

            int byteIndex = (int)(bitIndex / 8);
            int bitPosition = (int)(bitIndex % 8);

            if (byteIndex >= array.Length)
                throw new ArgumentOutOfRangeException();

            byte mask = (byte)(1 << bitPosition);

            if (value)
                array[byteIndex] |= mask;   // Set the bit
            else
                array[byteIndex] &= (byte)~mask;  // Clear the bit
        }

        public static bool GetBit(byte[] array, long bitIndex)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));
            if (bitIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(bitIndex), "Bit index must be non-negative.");

            // total number of addressable bits in the array
            long totalBits = (long)array.Length * 8;
            if (bitIndex >= totalBits)
                throw new ArgumentOutOfRangeException(nameof(bitIndex), $"Bit index must be less than {totalBits}.");

            // locate the byte and the bit within it
            int byteIndex = (int)(bitIndex >> 3);        // divide by 8
            int bitPosition = (int)(bitIndex & 0b111);     // mod 8

            byte mask = (byte)(1 << bitPosition);
            return (array[byteIndex] & mask) != 0;
        }

        public static string NormaliseString(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return value;
            }

            return value.ToLowerInvariant();
            //var result = value.Normalize(NormalizationForm.FormC).ToLowerInvariant();
            //return result;
        }

    }
}
