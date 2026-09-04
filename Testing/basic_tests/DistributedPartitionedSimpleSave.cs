
using LinqdbClient;
using LinqDbClientInternal;
using ServerLogic;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;

namespace Testing.basic_tests
{

    public class DistributedPartitionedSimpleSave : ITestDistributed
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
            //sids = sids.Select(f => new DistributedId(1, f.Id)).ToList();
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).DeleteNonAtomically(sids);

            var d = new SomeDataDistributed()
            {
                Id = 1,
                Sid = 1,
                Normalized = 1.2,
                PeriodId = 5
            };
            db.DistributedPartitionedTable<SomeDataDistributed>(partition).Save(d);

            var res = db.DistributedPartitionedTable<SomeDataDistributed>(partition).Select(f => new
            {
                PeriodId = f.PeriodId
            });
            if (res[0].PeriodId != 5)
            {
                throw new Exception("Assert failure");
            }
            if (dispose)
            {
                if (dispose) { Logic.Dispose(); }
            }
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
