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
    public class DistributedPartitionedBetween : ITestDistributed
    {
        public void Do(DistributedDb db)
        {
            string partition = "1";
            bool dispose = false;
            if (db == null)
            {
                db = SocketTestingDistributed.GetTestDb();
                dispose = true;
            }

            var sids = db.DistributedPartitionedTable<SomeDataDistributed>(partition).Select(f => new { f.Sid, f.Id }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).DeleteNonAtomically(sids);

            var d = new SomeDataDistributed()
            {
                Id = 1,
                Sid = 1,
                Normalized = 1.2,
                PeriodId = 5
            };
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).Save(d);
            d = new SomeDataDistributed()
            {
                Id = 2,
                Sid = 2,
                Normalized = 0.9,
                PeriodId = 7
            };
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).Save(d);
            d = new SomeDataDistributed()
            {
                Id = 3,
                Sid = 3,
                Normalized = 0.5,
                PeriodId = 10
            };
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).Save(d);
            d = new SomeDataDistributed()
            {
                Id = 4,
                Sid = 4,
                Normalized = 4.5,
                PeriodId = 15
            };
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).Save(d);

            var res = db.DistributedPartitionedTable<SomeDataDistributed>(partition)
                        .Between(f => f.Normalized, 0.1, 0.9)
                        .OrderBy(f => f.PeriodId)
                        .Select(f => new
                        {
                            f.PeriodId
                        });
            if (res.Count() != 2 || res[0].PeriodId + res[1].PeriodId != 17)
            {
                throw new Exception("Assert failure");
            }

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
