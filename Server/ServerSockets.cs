using LinqDbInternal;
using ServerSharedData;
using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Server
{
    //class Pinger
    //{
    //    private readonly CancellationToken _token;
    //    private readonly object _lock = new object();
    //    public BinaryWriter Writer { get; set; }

    //    public Pinger(BinaryWriter writer, CancellationToken token)
    //    {
    //        Writer = writer;
    //        _token = token;
    //    }

    //    public void Do()
    //    {
    //        try
    //        {
    //            int totalWait = 0;
    //            const int sleepMs = 2000;

    //            while (!_token.IsCancellationRequested)
    //            {
    //                Thread.Sleep(sleepMs);
    //                totalWait += sleepMs;

    //                if (totalWait % 10000 == 0)
    //                {
    //                    lock (_lock)
    //                    {
    //                        if (!_token.IsCancellationRequested && Writer != null)
    //                        {
    //                            Writer.Write(BitConverter.GetBytes(-2));
    //                            Writer.Flush();
    //                        }
    //                    }
    //                }
    //            }
    //        }
    //        catch
    //        {
    //            // best-effort keep-alive
    //        }
    //    }
    //}

    class Pinger
    {
        public bool Done { get; set; }
        public object _lock = new object();
        public BinaryWriter bw { get; set; }
        public void Do()
        {
            try
            {
                int total_wait = 0;
                int sleep_ms = 2000;
                while (!Done)
                {
                    Thread.Sleep(sleep_ms);
                    total_wait += sleep_ms;
                    if (total_wait % 10000 == 0)
                    {
                        lock (_lock)
                        {
                            if (!Done)
                            {
                                bw.Write(BitConverter.GetBytes(-2));
                                bw.Flush();
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                //var rg = new Random();
                //File.WriteAllText("pinger_error_" + rg.Next() + ".txt", ex.Message + " " + ex.StackTrace + (ex.InnerException != null ? (" " + ex.InnerException.Message + " " + ex.InnerException.StackTrace) : ""));
                return;
            }
        }
    }

    class ServerSockets
    {
        static readonly Socket listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        static readonly SocketAsyncEventArgs AcceptArgs = new SocketAsyncEventArgs(); // single reusable accept args

        static string db_path = null;
        static int port = 0;

        public static void Main()
        {
            ThreadPool.GetMinThreads(out var w, out var c);
            ThreadPool.SetMinThreads(Math.Max(w, 1000), Math.Max(c, 1000));

            AppDomain.CurrentDomain.ProcessExit += new EventHandler(OnProcessExit);

            CommandHelper.ReadConfig(out db_path, out port);

            var sw = Stopwatch.StartNew();
            Console.WriteLine("Building in-memory indexes...");
            SharedUtils.LogInfo(db_path, "Building in-memory indexes...");
            ServerLogic.Logic.ServerBuildIndexesOnStart(db_path);
            sw.Stop();
            Console.WriteLine("Done building in-memory indexes. It took: " + Math.Round(sw.ElapsedMilliseconds / 60000.0, 0) + " min.");
            SharedUtils.LogInfo(db_path, "Done building in-memory indexes. It took: " + Math.Round(sw.ElapsedMilliseconds / 60000.0, 0) + " min.");

            Console.WriteLine("Listening on " + port);
            SharedUtils.LogInfo(db_path, "Listening on " + port);

            listener.Bind(new IPEndPoint(IPAddress.Any, port));
            listener.Listen((int)SocketOptionName.MaxConnections);

            AcceptArgs.Completed += Service;
            StartAccept();

            // keep alive
            while (true)
            {
                try { Thread.Sleep(60000); }
                catch (Exception ex) { SharedUtils.LogError(db_path, "BAD ERROR...", ex); }
            }
        }

        static void OnProcessExit(object sender, EventArgs e)
        {
            try { ServerLogic.Logic.Dispose(); } catch { }
            try { AcceptArgs.Completed -= Service; } catch { }
            try { listener.Dispose(); } catch { }
        }

        private static void StartAccept()
        {
            try
            {
                AcceptArgs.AcceptSocket = null; // IMPORTANT when reusing
                if (!listener.AcceptAsync(AcceptArgs))
                {
                    // Completed synchronously
                    Service(null, AcceptArgs);
                }
            }
            catch (Exception ex)
            {
                SharedUtils.LogError(db_path, "Accept_start_error", ex);
                Task.Delay(50).ContinueWith(_ => StartAccept()); // gentle retry
            }
        }

        // Accept completion: repost once, then offload client handling
        private static void Service(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                if (e.SocketError != SocketError.Success || e.AcceptSocket == null)
                {
#if (VERBOSE)
                    Console.WriteLine("Accept error: " + e.SocketError);
#endif
                    try { e.AcceptSocket?.Dispose(); } catch { }
                    StartAccept();                 // repost once
                    return;
                }

                var s = e.AcceptSocket;
                StartAccept();                     // repost once immediately

                // Set basic options right away
                try
                {
                    s.NoDelay = true;
                    s.ReceiveTimeout = 60000;
                    s.SendTimeout = 60000;
                    // Optional: detect dead peers sooner if connections live long
                    // s.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                }
                catch { /* best-effort */ }

                // Offload to worker so IOCP thread isn’t tied up
                _ = Task.Run(() =>
                {
                    try { HandleClient(s); }
                    catch (Exception hx)
                    {
                        SharedUtils.LogError(db_path, "handle_client_error", hx);
                        try { s.Dispose(); } catch { }
                    }
                });
            }
            catch (Exception ex)
            {
                SharedUtils.LogError(db_path, "service_error", ex);
                // NOTE: do NOT repost here; we already reposted above
            }
        }

        // Read exactly 'count' bytes or throw IOE
        private static void ReadExact(Stream stream, byte[] buf, int ofs, int count)
        {
            int n = 0;
            while (n < count)
            {
                int r = stream.Read(buf, ofs + n, count - n);
                if (r <= 0) throw new IOException($"EOF while reading {count}-byte block; got {n}");
                n += r;
            }
        }

        private static void HandleClient(Socket soc)
        {
            using (soc) // socket owner
            using (Stream stream = new NetworkStream(soc, ownsSocket: false))
            using (BinaryWriter bw = new BinaryWriter(stream)) // if you want, use leaveOpen:true
            {
                const int ChunkSize = 8192;
                byte[] chunk = ArrayPool<byte>.Shared.Rent(ChunkSize);

                try
                {
                    while (true) // reuse same connection for many commands
                    {
                        // ---- Read 4-byte length prefix ----
                        int headerRead = 0;
                        try
                        {
                            ReadExact(stream, chunk, 0, 4);
                            headerRead = 4;
                        }
                        catch (IOException ioex)
                        {
                            // client closed or mid-operation kill — normal
                            //SharedUtils.LogInfo(db_path, $"read_header_eof from {soc.RemoteEndPoint}: {ioex.Message}");
                            return;
                        }
                        catch (Exception ex)
                        {
                            SharedUtils.LogError(db_path, $"read_header_error from {soc.RemoteEndPoint}", ex);
                            return;
                        }

                        int total = BitConverter.ToInt32(chunk, 0);

                        // Ping/pong path
                        if (total == -3)
                        {
                            try
                            {
                                bw.Write(BitConverter.GetBytes(-3));
                                bw.Flush();
                                continue;
                            }
                            catch (Exception ex)
                            {
                                SharedUtils.LogInfo(db_path, $"pong_write_failed to {soc.RemoteEndPoint}: {ex.Message}");
                                return;
                            }
                        }

                        if (total <= 0)
                        {
                            SharedUtils.LogInfo(db_path, $"bad_length {total} from {soc.RemoteEndPoint}");
                            return;
                        }

                        // ---- Read payload of 'total' bytes ----
                        byte[] payloadBuf = ArrayPool<byte>.Shared.Rent(total);
                        int received = 0;

                        try
                        {
                            //// If header+payload coalesced, we might already have some payload bytes in chunk[4..]
                            //int firstBatch = Math.Min(total, headerRead - 4); // usually 0
                            //if (firstBatch > 0)
                            //{
                            //    Buffer.BlockCopy(chunk, 4, payloadBuf, 0, firstBatch);
                            //    received = firstBatch;
                            //}

                            while (received < total)
                            {
                                int toRead = Math.Min(ChunkSize, total - received);
                                int read = stream.Read(chunk, 0, toRead);
                                if (read <= 0) throw new IOException("Unexpected EOF while reading payload.");
                                Buffer.BlockCopy(chunk, 0, payloadBuf, received, read);
                                received += read;
                            }

                            // Avoid copy if pool gave exact size; otherwise copy once
                            byte[] input = payloadBuf;
                            if (received != payloadBuf.Length)
                            {
                                input = new byte[received];
                                Buffer.BlockCopy(payloadBuf, 0, input, 0, received);
                            }

                            if (input.Length == 0) return;

                            //using (var pingerCts = new CancellationTokenSource())
                            //{
                            //    var pinger = new Pinger(bw, pingerCts.Token);
                            //    ThreadPool.QueueUserWorkItem(_ => pinger.Do());

                            //    byte[] output;
                            //    try
                            //    {
                            //        output = ServerLogic.Logic.Execute(input, db_path);
                            //    }
                            //    finally
                            //    {
                            //        pingerCts.Cancel();
                            //    }

                            //    // Respond (protect against interleaving with pinger)
                            //    lock (bw)
                            //    {
                            //        bw.Write(output);
                            //        bw.Flush();
                            //    }
                            //}


                            var pinger = new Pinger()
                            {
                                bw = bw
                            };
                            ThreadPool.QueueUserWorkItem(f => { pinger.Do(); });
                            byte[] output = null;
                            try
                            {
                                output = ServerLogic.Logic.Execute(input, db_path);
                            }
                            finally
                            {
                                pinger.Done = true;
                            }

                            lock (pinger._lock)
                            {
                                bw.Write(output);
                                bw.Flush();
                            }
                        }
                        catch (IOException ioex)
                        {
                            // Peer went away mid-payload => close cleanly
                            //SharedUtils.LogInfo(db_path, $"payload_read_eof from {soc.RemoteEndPoint}: {ioex.Message}");
                            return;
                        }
                        catch (Exception ex)
                        {
                            SharedUtils.LogError(db_path, $"payload_or_execute_error from {soc.RemoteEndPoint}", ex);
                            return;
                        }
                        finally
                        {
                            ArrayPool<byte>.Shared.Return(payloadBuf, clearArray: false);
                        }
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(chunk, clearArray: false);
                }
            }
        }
    }
}
