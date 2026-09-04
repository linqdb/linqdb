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
    class ParallelWrite : ITest
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


            var b = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                PeriodId = 5,
                StringVal = "test 123 abc"
            };
            db.Table<SomeData>().Save(b);
            b = new SomeData()
            {
                Id = 2,
                Normalized = 0.9,
                PeriodId = 7,
                StringVal = "test"
            };
            db.Table<SomeData>().Save(b);
            b = new SomeData()
            {
                Id = 3,
                Normalized = 0.5,
                PeriodId = 10
            };
            db.Table<SomeData>().Save(b);


            int total = 100000;
            Parallel.ForEach(Enumerable.Range(0, total).ToList(), new ParallelOptions()
            {
                MaxDegreeOfParallelism = 100
            }, z =>
            {
                var res = db.Table<SomeData>()
                            .Between(f => f.PeriodId, 1, 9)
                            .OrderBy(f => f.Id)
                            .Select(f => new
                            {
                                f.Id,
                                f.PeriodId
                            });
                if (res.Count() != 2 || res[0].Id != 1 || res[1].Id != 2)
                {
                    throw new Exception("Assert failure");
                }
                res = db.Table<SomeData>()
                        .Where(f => f.PeriodId == 5)
                        .Select(f => new
                        {
                            f.Id,
                            f.PeriodId
                        });
                if (res.Count() != 1 || res[0].Id != 1)
                {
                    throw new Exception("Assert failure");
                }

                var d = new SomeData()
                {
                    StringVal = "unique"
                };
                db.Table<SomeData>().Save(d);

            });

            var resnew = db.Table<SomeData>().Where(f => f.PeriodId == 0).GetIds();
            var count = db.Table<SomeData>().Count();

            if (resnew.Ids.Count() != total || count != total + 3)
            {
                throw new Exception("Assert failure");
            }

            db.Table<SomeData>().DeleteNonAtomically(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));

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
