using LinqdbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;

namespace Testing.basic_tests
{
    public class DistributedPartitionedSimpleWhere : ITestDistributed
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
                Id = 1,
                Sid= 1,
                Normalized = 2.1,
                PeriodId = 10
            };
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).Save(d); 

            var res = db.DistributedPartitionedTable<SomeDataDistributed>(partition)
                        .Where(f => f.PeriodId == 10)
                        .Select(f => new
                        {
                            Normalized = f.Normalized
                        });
            if (res[0].Normalized != 2.1)
            {
                throw new Exception(string.Format("Assert failure: {0} != 2.1", res[0].Normalized));
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

