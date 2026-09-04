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
    class DistributedWhere : ITestDistributed
    {
        public void Do(DistributedDb db)
        {
            bool dispose = false;
            if (db == null)
            {
                db = SocketTestingDistributed.GetTestDb();
                dispose = true;
            }

            var sids = db.DistributedTable<SomeDataDistributed>().Select(f => new { f.Sid, f.Id }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            db.DistributedTable<SomeDataDistributed>().DeleteNonAtomically(sids);

            var d = new SomeDataDistributed()
            {
                Gid = 1,
                Normalized = 1.2,
                PeriodId = 5,
                Date = Convert.ToDateTime("2020-01-01")
            };
            db.DistributedTable<SomeDataDistributed>().Save(d); 
            d = new SomeDataDistributed()
            {
                Gid = 2,
                Normalized = 2.3,
                PeriodId = 10,
                Date = Convert.ToDateTime("2024-01-01")
            };
            db.DistributedTable<SomeDataDistributed>().Save(d); 
            d = new SomeDataDistributed()
            {
                Gid = 3,
                Normalized = 4.5,
                PeriodId = 15,
                Date = Convert.ToDateTime("2001-01-01")
            };
            db.DistributedTable<SomeDataDistributed>().Save(d); 

            var res = db.DistributedTable<SomeDataDistributed>()
                    .Where(f => f.Date > Convert.ToDateTime("2016-11-12"))
                    .Select(f => new
                    {
                        f.PeriodId
                    });
    
            if (res.Count() != 2)
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