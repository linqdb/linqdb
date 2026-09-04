using LinqdbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;

namespace Testing.basic_tests
{
    public class DistributedBasicGroup5 : ITestDistributed
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
            db.DistributedTable<SomeDataDistributed>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Value);

            var sids = db.DistributedTable<SomeDataDistributed>().Select(f => new { f.Sid, f.Id }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            db.DistributedTable<SomeDataDistributed>().DeleteNonAtomically(sids);

            var d = new SomeDataDistributed()
            {
                Gid = 2,
                Normalized = 7,
                Value = 3,
                GroupBy = 3,
                NameSearch = "a"
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);

            d = new SomeDataDistributed()
            {
                Gid = 2,
                Normalized = 3,
                Value = 5,
                GroupBy = 3,
                NameSearch = "a"
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);

            d = new SomeDataDistributed()
            {
                Gid = 2,
                Normalized = 107,
                Value = 103,
                GroupBy = 5,
                NameSearch = "a"
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);

            d = new SomeDataDistributed()
            {
                Gid = 2,
                Normalized = 103,
                Value = 105,
                GroupBy = 5,
                NameSearch = "a"
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);


            db.DistributedTable<SomeDataDistributed>().CreateGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);
            db.DistributedTable<SomeDataDistributed>().CreateGroupByMemoryIndex(f => f.GroupBy, z => z.Value);

            var results = db.DistributedTable<SomeDataDistributed>()
                            .GroupBy(f => f.GroupBy)
                            .Select(f => new
                            {
                                f.Key
                            });

            if (results.Count() != 2)
            {
                throw new Exception("Assert failure");
            }

            var res = db.DistributedTable<SomeDataDistributed>()
                        .GroupBy(f => f.GroupBy)
                        .Select(f => new
                        {
                            f.Key,
                            Sum = f.Sum(z => z.Normalized),
                            Sum2 = f.Sum(z => z.Value),
                        });


            if (res.Count() != 2 || res.First(f => f.Key == 3).Sum != 10 || res.First(f => f.Key == 3).Sum2 != 8 ||
                res.First(f => f.Key == 5).Sum != 210 || res.First(f => f.Key == 5).Sum2 != 208)
            {
                throw new Exception("Assert failure");
            }

            db.DistributedTable<SomeDataDistributed>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);
            db.DistributedTable<SomeDataDistributed>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Value);


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
