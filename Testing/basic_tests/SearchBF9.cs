#if (SERVER || SOCKETS)
using LinqdbClient;
using ServerLogic;
#else
using LinqDb;
#endif
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    class SearchBF9 : ITest
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

#if (SOCKETS || SAMEDB || INDEXES || SERVER)
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif

#if INDEXES
            db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);
            db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Date);
            db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);
            db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.GroupBy2, z => z.Normalized);
            db.Table<SomeData>().RemovePropertyMemoryIndex(f => f.PeriodId);
            db.Table<SomeData>().RemovePropertyMemoryIndex(f => f.Value);
            db.Table<SomeData>().RemovePropertyMemoryIndex(f => f.Id);
            db.Table<SomeData>().RemoveBitFunnelMemoryIndex(f => f.StringVal);
#endif


           db.Table<SomeData>().CreatePropertyBitFunnelMemoryIndex(f => f.StringVal, 160, 1024*1024);
           
            var b = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                PeriodId = 1,
                StringVal = "test 123 abc",
                Value = 2
            };
            db.Table<SomeData>().Save(b);
            b = new SomeData()
            {
                Id = 2,
                Normalized = 0.9,
                PeriodId = 2,
                StringVal = "test",
                Value = 4
            };
            db.Table<SomeData>().Save(b);
            b = new SomeData()
            {
                Id = 3,
                Normalized = 0.5,
                PeriodId = 3,
                Value = 6
            };
            db.Table<SomeData>().Save(b);

            object _lock = new object();
            int counter = 0;

            int errors = 0;
            int total = 100000;
            Parallel.ForEach(Enumerable.Range(0, total).ToList(), new ParallelOptions()
            {
                MaxDegreeOfParallelism = 100
            }, z =>
            {
                try
                {
                    var res = db.Table<SomeData>()
                            .SearchBitFunnel(f => f.StringVal, "test")
                            .OrderBy(f => f.Id)
                            .Select(f => new
                            {
                                f.Id,
                                f.PeriodId
                            });
                    if (res.Count() != 2 || res[0].Id != 1 || res[1].Id != 2)
                    {
                        Console.WriteLine($"Error1: {res.Count()} {(res.Count() > 0 ? res[0].Id : -1)} {(res.Count() > 1 ? res[1].Id : -1)}");
                        throw new Exception("Assert failure");
                    }
                    res = db.Table<SomeData>()
                            .SearchBitFunnel(f => f.StringVal, "test abc")
                            .Select(f => new
                            {
                                f.Id,
                                f.PeriodId
                            });
                    if (res.Count() != 1 || res[0].Id != 1)
                    {
                        Console.WriteLine($"Error2: {res.Count()} {(res.Any() ? res[0].Id : -1)}");
                        throw new Exception("Assert failure");
                    }

                    lock (_lock)
                    {
                        counter++;
                    }

                    var rg = new Random();
                    var next = rg.Next(1, 4);
                    var d = new SomeData()
                    {
                        StringVal = "unique",
                        PeriodId = next,
                        Value = next * 2
                    };
                    db.Table<SomeData>().Save(d);


                    var ids = db.Table<SomeData>()
                                .SearchBitFunnel(f => f.StringVal, "unique")
                                .GetIds().Ids;


                    if (ids.Count() < counter - 1000 || ids.Count() > counter)
                    {
                        Console.WriteLine($"Error3: {ids.Count()} {counter}");
                        throw new Exception("Assert failure");
                    }
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        errors++;
                    }
                    Console.WriteLine("Exception: " + ex.Message);
                }                
            });

            if (errors > 0)
            {
                throw new Exception("Assert failure");
            }
            var resnew = db.Table<SomeData>()
                            .SearchBitFunnel(f => f.StringVal, "unique")
                            .GetIds();


            if (resnew.Ids.Count() != total)
            {
                throw new Exception("Assert failure");
            }

            db.Table<SomeData>().DeleteNonAtomically(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));

            var count = db.Table<SomeData>().Count();
            if (count != 0)
            {
                throw new Exception("Assert failure");
            }

            db.Table<SomeData>().RemoveBitFunnelMemoryIndex(f => f.StringVal);


            count = db.Table<SomeData>().Count();
            if (count != 0)
            {
                throw new Exception("Assert failure");
            }

#if (SERVER || SOCKETS)
            if(dispose) { Logic.Dispose(); }
#else
            if (dispose) { db.Dispose(); }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
#endif
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
