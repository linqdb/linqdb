#if (SERVER || SOCKETS)
using LinqdbClient;
using LinqDbClientInternal;
using ServerLogic;
#else
using LinqDb;
#endif

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    public class PartitionedSimpleSave : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false;
            string partition = "1";
            string partition2 = "2";

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
            db.PartitionedTable<SomeData>(partition).Delete(new HashSet<int>(db.PartitionedTable<SomeData>(partition).Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.PartitionedTable<SomeData>(partition2).Delete(new HashSet<int>(db.PartitionedTable<SomeData>(partition2).Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif

            var sd = db.Table<SomeData>().SelectEntity();

            var d = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                PeriodId = 5
            };
            db.PartitionedTable<SomeData>(partition).Save(d);

            var res = db.PartitionedTable<SomeData>(partition).Select(f => new
            {
                Id = f.Id,
                PeriodId = f.PeriodId
            });
            if (res[0].PeriodId != 5 || res[0].Id != 1)
            {
                throw new Exception("Assert failure");
            }


            sd = db.Table<SomeData>().SelectEntity();

            if (sd.Count() > 0)
            {
                throw new Exception("Assert failure");
            }

            db.PartitionedTable<SomeData>(partition2).Save(d);
            var res2 = db.PartitionedTable<SomeData>(partition2).Select(f => new
            {
                Id = f.Id,
                PeriodId = f.PeriodId
            });

            if (res2[0].PeriodId != 5 || res2[0].Id != 1)
            {
                throw new Exception("Assert failure");
            }

            var tables = db.GetTables();
            if (!tables.Contains($"SomeData_{partition}") || !tables.Contains($"SomeData_{partition}"))
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
            if(dispose)
            {
                if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
            }
#endif

        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
