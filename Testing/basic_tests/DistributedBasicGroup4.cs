using LinqdbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;

namespace Testing.basic_tests
{
    public class DistributedBasicGroup4 : ITestDistributed
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

            var d = new SomeDataDistributed()
            {
                Gid = 2,
                Normalized = 7,
                GroupBy = 3,
                NameSearch = "a"
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);


            db.DistributedTable<SomeDataDistributed>().CreateGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);

            try
            {
                var res = db.DistributedTable<SomeDataDistributed>()
                            .GroupBy(f => f.GroupBy)
                            .Select(f => new
                            {
                                f.Key,
                                Sum = f.Average(z => z.Normalized),
                                Count = f.Count(),
                            });
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("is not supported"))
                {
                    throw new Exception("Assert failure");
                }
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
