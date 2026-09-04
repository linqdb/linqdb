using LinqdbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;
using Testing.tables;

namespace Testing.basic_tests
{
    class DistributedVectorSearchStats : ITestDistributed
    {
        public void Do(DistributedDb db)
        {
            bool dispose = false;
            if (db == null)
            {
                db = SocketTestingDistributed.GetTestDb();
                dispose = true;
            }

            var sids = db.DistributedTable<SomeVectorDistributed>().Select(f => new { f.Sid, f.Id }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            db.DistributedTable<SomeVectorDistributed>().DeleteNonAtomically(sids);

            int total = 10000;
            //#if (DATA)
            //    total = 102000;
            //#endif
            var list = new List<SomeVectorDistributed>();
            for (int i = 0; i < total; i++)
            {
                list.Add(new SomeVectorDistributed()
                {
                    VectorL2 = VecOps.RandomVector(768)
                });
                if (list.Count() > 1000)
                {
                    db.DistributedTable<SomeVectorDistributed>().SaveBatch(list);
                    list = new List<SomeVectorDistributed>();
                }
            }

            if (list.Count() > 0)
            {
                db.DistributedTable<SomeVectorDistributed>().SaveBatch(list);
                list = new List<SomeVectorDistributed>();
            }

            var count = db.DistributedTable<SomeVectorDistributed>().Count();

            var vector1 = Enumerable.Range(0, 768).Select(f => (float)1).ToArray();
            LinqdbSelectStatistics stats = new LinqdbSelectStatistics();
            var res = db.DistributedTable<SomeVectorDistributed>().SearchNeighbours(f => f.VectorL2, vector1, 10).SelectEntity(stats);

            res = res.OrderBy(f => stats.DistancesDist[new Tuple<int, int>(f.Sid, f.Id)]).Take(10).ToList();

            double prev_distance = 0;
            foreach (var r in res)
            {
                var stats_distance = (long)(Math.Sqrt(stats.DistancesDist[new Tuple<int, int>(r.Sid, r.Id)]) * 10000);
                var calculated_distance = (long)(VecOps.L2(vector1, r.VectorL2) * 10000);
                if (stats_distance != calculated_distance)
                {
                    throw new Exception("Assert failure");
                }

                if (stats_distance < prev_distance)
                {
                    throw new Exception("Assert failure");
                }
                prev_distance = stats_distance;
            }

            db.DistributedTable<SomeVectorDistributed>().DeleteNonAtomically(db.DistributedTable<SomeVectorDistributed>().Select(f => new { f.Sid, f.Id }).Select(f => new DistributedId(f.Sid, f.Id)).ToList());


            if (dispose)
            {
                if (dispose) { db.Dispose(); }
            }
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
