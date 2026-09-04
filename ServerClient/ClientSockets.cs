using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace LinqDbClientInternal
{
    public class TcpConnection
    {
        private int _busy; // 0 = free, 1 = busy
        public bool TakeLock() => Interlocked.Exchange(ref _busy, 1) == 0;
        public void ReleaseLock() => Volatile.Write(ref _busy, 0);

        public bool Connected { get; set; }
        public string ConnError { get; set; }
        public Socket client { get; set; }
        public Stream stream { get; set; }
        public BinaryWriter bw { get; set; }
        public DateTime LastUsed { get; set; }
        public int Index { get; set; }

        // Per-connection reusable scratch buffer (no ArrayPool in .NET 4.8)
        // Safe because this connection is guarded by TakeLock/ReleaseLock.
        public readonly byte[] Scratch = new byte[1024];

        public TcpConnection(string hostname, int port)
        {
            client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true,
                ReceiveTimeout = 60000,
                SendTimeout = 60000
            };

            // Allow DNS names too; prefer IPv4 like original code
            IPAddress ip;
            if (!IPAddress.TryParse(hostname, out ip))
                ip = Dns.GetHostAddresses(hostname).First(a => a.AddressFamily == AddressFamily.InterNetwork);

            client.Connect(new IPEndPoint(ip, port));

            // You own the socket; NetworkStream should not close it.
            this.stream = new NetworkStream(client, ownsSocket: false);
            this.bw = new BinaryWriter(stream);
            this.Connected = client.Connected;

#if (VERBOSE)
            if (client.Connected) Console.WriteLine("Connected immediately");
#endif

            TakeLock(); // start as busy for the first use
            LastUsed = DateTime.Now;
        }
    }

    public class ClientSockets
    {
        const int _limit = 100;
        TcpConnection[] cons = new TcpConnection[_limit];
        object _lock = new object();
        object[] _locks = null;

        public byte[] CallServer(byte[] input, string hostname, int port, out string error_msg)
        {
            return CallServerImpl(input, hostname, port, out error_msg);
        }

        private byte[] CallServerImpl(byte[] input, string hostname, int port, out string error_msg)
        {
            error_msg = null;

            // lazy-init per-slot locks
            if (_locks == null)
            {
                lock (_lock)
                {
                    if (_locks == null)
                    {
                        _locks = new object[_limit];
                        for (int i = 0; i < _limit; i++) _locks[i] = new object();
                    }
                }
            }

            TcpConnection conn = null;

            // Acquire a pooled connection or create a new one
            while (true)
            {
                int last_index = 0;
                for (int i = _limit - 1; i >= 0; i--)
                {
                    if (cons[i] != null) { last_index = i; break; }
                }

                for (int i = 0; i < _limit; i++)
                {
                    var tmp = cons[i];
                    if (tmp != null)
                    {
                        if (!tmp.TakeLock()) continue;

                        // Evict stale (>30s idle)
                        if ((DateTime.Now - tmp.LastUsed).TotalSeconds > 30)
                        {
                            cons[i] = null;
                            SafeDispose(tmp);
                            continue;
                        }

                        // --- ping using connection's scratch to validate connection ---
                        if (!Ping(tmp))
                        {
                            cons[i] = null;
                            SafeDispose(tmp);
                            continue;
                        }

                        conn = tmp; // good to use
                        break;
                    }
                    else
                    {
                        if (i < last_index) continue;

                        if (Monitor.TryEnter(_locks[i]))
                        {
                            try
                            {
                                if (cons[i] != null) continue; // lost race

                                conn = new TcpConnection(hostname, port);
                                cons[i] = conn;
                                conn.Index = i;
                                break;
                            }
                            catch (Exception ex)
                            {
                                conn = null;
                                cons[i] = null;
#if (VERBOSE)
                                Console.WriteLine("Client socket creation error: " + ex.Message);
#endif
                                error_msg = ex.Message;
                                return BitConverter.GetBytes(-1);
                            }
                            finally
                            {
                                Monitor.Exit(_locks[i]);
                            }
                        }
                        else
                        {
                            continue;
                        }
                    }
                }

                if (conn == null)
                {
                    Thread.Sleep(150);
                    continue;
                }
                else
                {
                    break;
                }
            }

            bool error = false;

            try
            {
                // Write request as-is; higher layer frames it (your protocol handles length elsewhere).
                conn.bw.Write(input);
                conn.bw.Flush();

                // Read response (header + payload) using the per-connection scratch buffer
                var resp = ReadFramedResponse(conn.stream, conn.Scratch);
                conn.LastUsed = DateTime.Now;
                return resp;
            }
            catch (IOException ioEx) // NetworkStream wraps SocketException
            {
                error = true;
                error_msg = ioEx.Message;
                return BitConverter.GetBytes(-1);
            }
            catch (SocketException sx)
            {
                error = true;
                error_msg = sx.Message;
                return BitConverter.GetBytes(-1);
            }
            catch (Exception ex)
            {
                error = true;
                error_msg = ex.Message;
                return BitConverter.GetBytes(-1);
            }
            finally
            {
                if (!error)
                {
                    conn?.ReleaseLock();
                }
                else if (conn != null)
                {
                    // Evict broken connection from pool
                    cons[conn.Index] = null;
                    SafeDispose(conn);
                }
            }
        }

        // Validate an existing pooled connection by round-tripping a ping (-3/-3)
        private bool Ping(TcpConnection tmp)
        {
            var scratch = tmp.Scratch;
            try
            {
                tmp.bw.Write(BitConverter.GetBytes(-3));
                tmp.bw.Flush();

                int numBytesRead = 0;
                while (numBytesRead < 4)
                {
                    int read;
                    try { read = tmp.stream.Read(scratch, numBytesRead, 4 - numBytesRead); }
                    catch { return false; }
                    if (read <= 0) return false;
                    numBytesRead += read;
                }

                int pong = BitConverter.ToInt32(scratch, 0);
                return pong == -3;
            }
            catch
            {
                return false;
            }
        }

        // Reads a framed response: 4-byte length header (or -2 pinger) + payload
        private byte[] ReadFramedResponse(Stream stream, byte[] scratch)
        {
            const int ScratchSize = 1024;
            if (scratch == null || scratch.Length < ScratchSize)
                scratch = new byte[ScratchSize]; // fallback (shouldn't happen)

            int numBytesRead;
            int total;

            // read 4-byte header; ignore keepalive (-2) frames
            while (true)
            {
                numBytesRead = 0;
                while (numBytesRead < 4)
                {
                    int read = stream.Read(scratch, numBytesRead, 4 - numBytesRead);
                    if (read <= 0) throw new LinqDbException("Read <= 0 while reading header");
                    numBytesRead += read;
                }

                total = BitConverter.ToInt32(scratch, 0);
                if (total == -2)
                {
#if (VERBOSE)
                    Console.WriteLine("PINGER!!!");
#endif
                    continue; // read next header
                }
                break;
            }

            if (total < 0) throw new LinqDbException("Bad length " + total);

            // If the server coalesced header+payload into one recv, we may already have bytes for payload in scratch[4..]
            int already = numBytesRead - 4; // usually 0
            byte[] resp = new byte[total];
            int copied = 0;

            if (already > 0)
            {
                int firstBatch = Math.Min(already, total);
                Buffer.BlockCopy(scratch, 4, resp, 0, firstBatch);
                copied = firstBatch;
            }

            while (copied < total)
            {
                int toRead = Math.Min(ScratchSize, total - copied);
                int read = stream.Read(scratch, 0, toRead);
                if (read <= 0) throw new LinqDbException("Read <= 0 while reading payload");
                Buffer.BlockCopy(scratch, 0, resp, copied, read);
                copied += read;
            }

            return resp;
        }

        private void SafeDispose(TcpConnection c)
        {
            try { c?.bw?.Dispose(); } catch { }
            try { c?.stream?.Dispose(); } catch { }
            try { c?.client?.Shutdown(SocketShutdown.Both); } catch { }
            try { c?.client?.Dispose(); } catch { }
        }
    }
}
