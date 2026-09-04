#if (SERVER || SOCKETS)
using LinqdbClient;
using ServerLogic;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Testing.Queues;
using Testing.tables;

namespace Testing.basic_tests
{
    public class QueuesPutGet : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false; if (db == null) { db = new Db("DATA"); dispose = true; }
#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif

            db.Queue<QueueA>().GetAllFromQueue();

            int total = 30000;
            int read = 0;
            object _read_lock = new object();

            for(int i=0; i<50; i++)
            {
                ThreadPool.QueueUserWorkItem(_ =>
                {
                    while (read < total)
                    {
                        try
                        {
                            var res = db.Queue<QueueA>().GetAllFromQueue();
                            if (res.Any() && Convert.FromBase64String(res.First().SomeArray).Count() != 3)
                            {
                                throw new Exception("not match");
                            }
                            var count = res.Count();
                            if(count > 0)
                            {
                                lock (_read_lock)
                                {
                                    read += count;
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("A " + ex.Message);
                        }
                    }
                });
            }


            // Parallel.ForEach(Enumerable.Range(0, 100).ToList(), f => {

            //     ThreadPool.QueueUserWorkItem(_ =>
            //     {
            //         while (read < total)
            //         {
            //             try
            //             {
            //                 var res = db.Queue<QueueA>().GetAllFromQueue();
            //                 if (res.Any() && Convert.FromBase64String(res.First().SomeArray).Count() != 3)
            //                 {
            //                     throw new Exception("not match");
            //                 }
            //                 lock (_read_lock)
            //                 {
            //                     read += res.Count();
            //                 }
            //             }
            //             catch (Exception ex)
            //             {
            //                 Console.WriteLine("A " + ex.Message);
            //             }
            //         }
            //     });                    
            // });

            for(int i=0; i<total; i++)
            {
                var thread = new Thread(() =>
                {
                    try
                    {
                        var a = new QueueA()
                        {
                            SomeString = "test123",
                            SomeArray = Convert.ToBase64String(new List<byte>() { 1, 2, 3 }.ToArray())
                        };

                        db.Queue<QueueA>().PutToQueue(a);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                });

                if (RuntimeInformation.FrameworkDescription.StartsWith(".NET Framework"))
                {
                    thread.Priority = ThreadPriority.Normal;
                }
                else
                {
                    thread.Priority = ThreadPriority.Lowest;
                }
                
                thread.Start();
            };


            Thread.Sleep(5000);


            if (read != total)
            {
                throw new Exception("Assert failure");
            }

#if (SERVER || SOCKETS)
            if (dispose) { Logic.Dispose(); }
#else
            if (dispose) { db.Dispose(); }
#endif
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
#endif