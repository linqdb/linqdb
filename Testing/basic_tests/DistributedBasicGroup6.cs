using LinqdbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;

namespace Testing.basic_tests
{
    public class DistributedBasicGroup6 : ITestDistributed
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
            db.DistributedTable<SomeDataDistributed>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Date);

            var sids = db.DistributedTable<SomeDataDistributed>().Select(f => new { f.Sid, f.Id }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            db.DistributedTable<SomeDataDistributed>().DeleteNonAtomically(sids);

            var d = new SomeDataDistributed()
            {
                Gid = 2,
                Normalized = 7,
                Value = 3,
                GroupBy = 3,
                NameSearch = "a",
                Date = Convert.ToDateTime("2001-01-01")
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);

            d = new SomeDataDistributed()
            {
                Gid = 3,
                Normalized = 3,
                Value = 5,
                GroupBy = 3,
                NameSearch = "a",
                Date = Convert.ToDateTime("2020-01-01")
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);

            d = new SomeDataDistributed()
            {
                Gid = 4,
                Normalized = 107,
                Value = 103,
                GroupBy = 5,
                NameSearch = "a",
                Date = Convert.ToDateTime("2010-01-01")
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);

            d = new SomeDataDistributed()
            {
                Gid = 5,
                Normalized = 103,
                Value = 105,
                GroupBy = 5,
                NameSearch = "a",
                Date = Convert.ToDateTime("2011-01-01")
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);


            db.DistributedTable<SomeDataDistributed>().CreateGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);
            db.DistributedTable<SomeDataDistributed>().CreateGroupByMemoryIndex(f => f.GroupBy, z => z.Value);
            db.DistributedTable<SomeDataDistributed>().CreateGroupByMemoryIndex(f => f.GroupBy, z => z.Date);

            var res = db.DistributedTable<SomeDataDistributed>()
                            .Intersect(f => f.Gid, new HashSet<int>() { 2, 3, 5 })
                            .GroupBy(f => f.GroupBy)
                            .Select(f => new
                            {
                                f.Key,
                                MinDate = f.Min(z => z.Date),
                                MaxDate = f.Max(z => z.Date)
                            });

            if (res.Count() != 2 || res.First(f => f.Key == 3).MinDate != Convert.ToDateTime("2001-01-01") || res.First(f => f.Key == 3).MaxDate != Convert.ToDateTime("2020-01-01") ||
                res.First(f => f.Key == 5).MinDate != Convert.ToDateTime("2011-01-01") || res.First(f => f.Key == 5).MaxDate != Convert.ToDateTime("2011-01-01"))
            {
                throw new Exception("Assert failure");
            }

            db.DistributedTable<SomeDataDistributed>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);
            db.DistributedTable<SomeDataDistributed>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Value);
            db.DistributedTable<SomeDataDistributed>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Date);


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
