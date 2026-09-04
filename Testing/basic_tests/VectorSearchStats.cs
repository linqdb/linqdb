#if (SERVER || SOCKETS)
using LinqdbClient;
using ServerLogic;
#else
using LinqDb;
#endif
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    class VectorSearchStats : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false;
            if (db == null)
            {
                db = new Db("DATA");
                dispose = true;
            }
#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif
            db.Table<SomeVector>().DeleteNonAtomically(new HashSet<int>(db.Table<SomeVector>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            int total = 1000;
            //#if (DATA)
            //    total = 102000;
            //#endif
            var list = new List<SomeVector>();
            for (int i = 0; i < total; i++)
            {
                list.Add(new SomeVector()
                {
                    VectorL2 = VecOps.RandomVector(768)
                });
                if (list.Count() > 100)
                {
                    db.Table<SomeVector>().SaveBatch(list);
                    list = new List<SomeVector>();
                }
            }

            if (list.Count() > 0)
            {
                db.Table<SomeVector>().SaveBatch(list);
                list = new List<SomeVector>();
            }

            var count = db.Table<SomeVector>().Count();

            var vector1 = Enumerable.Range(0, 768).Select(f => (float)1).ToArray();
            LinqdbSelectStatistics stats = new LinqdbSelectStatistics();
            var res = db.Table<SomeVector>().SearchNeighbours(f => f.VectorL2, vector1, 10).SelectEntity(stats);

            foreach (var r in res)
            {
                var stats_distance = Math.Round(Math.Sqrt(stats.Distances[r.Id]), 3);
                var calculated_distance = Math.Round(VecOps.L2(vector1, r.VectorL2), 3);
                if ((long)(stats_distance * 1000) != (long)(calculated_distance * 1000))
                {
                    throw new Exception("Assert failure");
                }
            }

            db.Table<SomeVector>().DeleteNonAtomically(new HashSet<int>(db.Table<SomeVector>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));

#if (SERVER || SOCKETS)
            if (dispose)
            {
                if(dispose) { Logic.Dispose(); }
            }
#else
            if (dispose)
            {
                if (dispose) { db.Dispose(); }
            }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if(dispose)
            {
                if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
            }
#endif
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
