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
    class SearchBF : ITest
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
#endif

            db.Table<SomeData>().CreatePropertyBitFunnelMemoryIndex(f => f.StringVal, 160);

            var d = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                PeriodId = 5,
                StringVal = "test 123 abc"
            };
            db.Table<SomeData>().Save(d);
            d = new SomeData()
            {
                Id = 2,
                Normalized = 0.9,
                PeriodId = 7,
                StringVal = "test"
            };
            db.Table<SomeData>().Save(d);
            d = new SomeData()
            {
                Id = 3,
                Normalized = 0.5,
                PeriodId = 10
            };
            db.Table<SomeData>().Save(d);


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
                throw new Exception("Assert failure");
            }

            db.Table<SomeData>().RemoveBitFunnelMemoryIndex(f => f.StringVal);
            db.Table<SomeData>().RemoveBitFunnelMemoryIndex(f => f.StringVal);
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
