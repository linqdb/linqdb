using LinqdbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;

namespace Testing.basic_tests
{
    public class DistributedBasicGroup2 : ITestDistributed
    {
        public void Do(DistributedDb db)
        {
            bool dispose = false;
            if (db == null)
            {
                db = SocketTestingDistributed.GetTestDb();
                dispose = true;
            }

            db.DistributedTable<SomeDataDistributed>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);

            var sids = db.DistributedTable<SomeDataDistributed>().Select(f => new { f.Sid, f.Id }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            db.DistributedTable<SomeDataDistributed>().DeleteNonAtomically(sids);

            var list = new List<SomeDataDistributed>();
            for (int i = 0; i < 1000000; i++)
            {
                var item = new SomeDataDistributed()
                {
                    Gid = i,
                    Normalized = 1,
                    GroupBy = 5,
                    NameSearch = "a"
                };
                list.Add(item);
            }
            db.DistributedTable<SomeDataDistributed>().SaveBatchNonAtomically(list);

            var d = new SomeDataDistributed()
            {
                Gid = 2,
                Normalized = 7,
                GroupBy = 3,
                NameSearch = "a"
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);


            db.DistributedTable<SomeDataDistributed>().CreateGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);

            var res = db.DistributedTable<SomeDataDistributed>()
                        .GroupBy(f => f.GroupBy)
                        .Select(f => new
                        {
                            f.Key,
                            Sum = f.Sum(z => z.Normalized),
                            Count = f.Count(),
                        });


            if (res.Count() != 2 || res.Where(f => f.Key == 5).First().Count != 1000000 || res.Where(f => f.Key == 3).First().Count != 1 ||
                res.Where(f => f.Key == 5).First().Sum != 1000000 || res.Where(f => f.Key == 3).First().Sum != 7)
            {
                throw new Exception("Assert failure");
            }

            db.DistributedTable<SomeDataDistributed>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);


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
