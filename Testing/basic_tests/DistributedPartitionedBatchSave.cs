using LinqdbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;

namespace Testing.basic_tests
{
    public class DistributedPartitionedBatchSave : ITestDistributed
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

            int total = 3000;
            var list = new List<SomeDataDistributed>();
            for (int i = 0; i < total; i++)
            {
                list.Add(new SomeDataDistributed()
                {
                    Normalized = 1.2,
                    PeriodId = i,
                    NameSearch = "test_" + i + " 123_" + i
                });
            }

            db.DistributedPartitionedTable<SomeDataDistributed>(partition).SaveBatch(list);

            var res = db.DistributedPartitionedTable<SomeDataDistributed>(partition)
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId
                        });
            
            if (res.Count() != total)
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
