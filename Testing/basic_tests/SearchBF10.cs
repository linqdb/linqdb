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
    class SearchBF10 : ITest
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

            var list = new List<SomeData>();
            for (int i = 0; i < 100000; i++)
            {
                list.Add(new SomeData()
                {
                    PeriodId = i,
                    StringVal = "5555"
                });
            }

            db.Table<SomeData>().SaveNonAtomically(list);

            var ratio = db.Table<SomeData>().CreatePropertyBitFunnelMemoryIndex(f => f.StringVal, 160, 1024*50);

            if (Math.Round(ratio, 2) != 0.02)
            {
                throw new Exception("Assert failure");
            }

            var val = 5555;
            var total = db.Table<SomeData>()
                        .SearchBitFunnel(f => f.StringVal, val.ToString())
                        .Count();

            if (total != 100000)
            {
                throw new Exception("Assert failure");
            }

            total = db.Table<SomeData>()
                       .SearchBitFunnel(f => f.StringVal, val.ToString(), 5000)
                       .Count();

            if (total < 50000 || total > 55000)
            {
                throw new Exception("Assert failure");
            }

            db.Table<SomeData>().RemoveBitFunnelMemoryIndex(f => f.StringVal);

            db.Table<SomeData>().DeleteNonAtomically(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));

            var count = db.Table<SomeData>().Count();
            if (count != 0)
            {
                throw new Exception("Assert failure");
            }

#if (SERVER || SOCKETS)
            if (dispose) { Logic.Dispose(); }
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
