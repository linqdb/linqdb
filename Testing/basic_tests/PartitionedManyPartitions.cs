#if (SERVER || SOCKETS)
using LinqdbClient;
using LinqDbClientInternal;
using ServerLogic;
#else
using LinqDb;
#endif

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    public class PartitionedManyPartitions : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false;
            var total = 70000;
            if (db == null)
            {
                db = new Db("DATA");
                dispose = true;
            }
#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif
#if (SOCKETS || SAMEDB || INDEXES || SERVER)
            for (int i = 0; i < total; i++)
            {
                db.PartitionedTable<SomeData>(i.ToString()).Delete(new HashSet<int>(db.PartitionedTable<SomeData>(i.ToString()).Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            }
#endif

            for (int i = 0; i < total; i++)
            {
                db.PartitionedTable<SomeData>(i.ToString()).Save(new SomeData()
                { 
                    PeriodId = i
                });
            }

            var ts = db.GetTables();

            if (ts.Count() < total)
            {
                throw new Exception("Assert failure");
            }

            for (int i = 0; i < total; i++)
            {
                var r = db.PartitionedTable<SomeData>(i.ToString()).SelectEntity();
                if (r.Count() != 1)
                {
                    throw new Exception("Assert failure");
                }
            }
            for (int i = 0; i < total; i++)
            {
                var r = db.PartitionedTable<SomeData>(i.ToString()).SelectEntity();
                if ( r.First().PeriodId != i)
                {
                    throw new Exception("Assert failure");
                }
            }

            for (int i = 0; i < 10; i++)
            {
                db.PartitionedTable<SomeData>("65536").Save(new SomeData()
                { 
                    PeriodId = 65536
                });
            }

            var res = db.PartitionedTable<SomeData>("65536").SelectEntity();
            if (res.Count() != 11)
            {
                throw new Exception("Assert failure");
            }
            for (int i = 0; i < 5; i++)
            {
                db.PartitionedTable<SomeData>("0").Save(new SomeData()
                {
                    PeriodId = 0
                });
            }
            res = db.PartitionedTable<SomeData>("0").SelectEntity();
            if (res.Count() != 6)
            {
                throw new Exception("Assert failure");
            }

#if (SERVER || SOCKETS)
            if (dispose)
            {
                if(dispose) { Logic.Dispose(); }
            }
#else
            if (dispose)
            {
                if (dispose) { db.Dispose(); }
            }
#endif


#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if (dispose)
            {
                if (dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
            }
#endif

        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
