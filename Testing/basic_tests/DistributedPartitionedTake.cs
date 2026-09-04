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
    public class DistributedPartitionedTake : ITestDistributed
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
                Id = 11,
                Sid = 1,
                Normalized = 1.2,
                PeriodId = 5
            };
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).Save(d);
            d = new SomeDataDistributed()
            {
                Id = 111,
                Sid = 1,
                Normalized = 1.2,
                PeriodId = 5
            };
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).Save(d);
            d = new SomeDataDistributed()
            {
                Id = 1111,
                Sid = 1,
                Normalized = 1.2,
                PeriodId = 5
            };
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).Save(d);

            d = new SomeDataDistributed()
            {
                Id = 2,
                Sid = 2,
                Normalized = 2.3,
                PeriodId = 7
            };
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).Save(d);
            d = new SomeDataDistributed()
            {
                Id = 22,
                Sid = 2,
                Normalized = 2.3,
                PeriodId = 7
            };
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).Save(d);
            d = new SomeDataDistributed()
            {
                Id = 222,
                Sid = 2,
                Normalized = 2.3,
                PeriodId = 7
            };
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).Save(d);
            d = new SomeDataDistributed()
            {
                Id = 2222,
                Sid = 2,
                Normalized = 2.3,
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
                Id = 33,
                Sid = 3,
                Normalized = 0.5,
                PeriodId = 10
            };
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).Save(d);
            d = new SomeDataDistributed()
            {
                Id = 333,
                Sid = 3,
                Normalized = 0.5,
                PeriodId = 10
            };
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).Save(d);
            d = new SomeDataDistributed()
            {
                Id = 3333,
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
            d = new SomeDataDistributed()
            {
                Id = 44,
                Sid = 4,
                Normalized = 4.5,
                PeriodId = 15
            };
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).Save(d);
            d = new SomeDataDistributed()
            {
                Id = 444,
                Sid = 4,
                Normalized = 4.5,
                PeriodId = 15
            };
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).Save(d);
            d = new SomeDataDistributed()
            {
                Id = 4444,
                Sid = 4,
                Normalized = 4.5,
                PeriodId = 15
            };
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).Save(d);

            var res = db.DistributedPartitionedTable<SomeDataDistributed>(partition)
                        .OrderBy(f => f.Normalized)
                        .Take(3)
                        .Select(f => new
                        {
                            f.PeriodId,
                            f.Normalized
                        });

            if (res.Count() != 3)
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
