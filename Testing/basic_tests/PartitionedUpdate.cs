#if (SERVER || SOCKETS)
using LinqdbClient;
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
    class PartitionedUpdate : ITest
    {
        public void Do(Db db)
        {
            string partition = "1";
            bool dispose = false; if (db == null) { db = new Db("DATA"); dispose = true; }
#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif
#if (SOCKETS || SAMEDB || INDEXES || SERVER)
            db.PartitionedTable<SomeData>(partition).Delete(new HashSet<int>(db.PartitionedTable<SomeData>(partition).Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
            var d = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                PeriodId = 5
            };
            db.PartitionedTable<SomeData>(partition).Save(d);;
            d = new SomeData()
            {
                Id = 2,
                Normalized = 0.9,
                PeriodId = 7
            };
            db.PartitionedTable<SomeData>(partition).Save(d);;
            d = new SomeData()
            {
                Id = 3,
                Normalized = 0.5,
                PeriodId = 10
            };
            db.PartitionedTable<SomeData>(partition).Save(d);;
            d = new SomeData()
            {
                Id = 4,
                Normalized = 4.5,
                PeriodId = 15
            };
            db.PartitionedTable<SomeData>(partition).Save(d);;

            var res = db.PartitionedTable<SomeData>(partition)
                        .Between(f => f.Normalized, 0.1, 0.9)
                        .OrderBy(f => f.PeriodId)
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 2 || res[0].PeriodId != 7 || res[1].PeriodId != 10)
            {
                throw new Exception("Assert failure");
            }

            var dic = new Dictionary<int, int?>();
            dic[2] = 8;
            dic[3] = 11;
            db.PartitionedTable<SomeData>(partition).Update(f => f.PeriodId, dic);

            res = db.PartitionedTable<SomeData>(partition)
                        .Between(f => f.Normalized, 0.1, 0.9)
                        .OrderBy(f => f.PeriodId)
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 2 || res[0].PeriodId != 8 || res[1].PeriodId != 11)
            {
                throw new Exception("Assert failure");
            }

            res = db.PartitionedTable<SomeData>(partition)
                       .Select(f => new
                       {
                           PeriodId = f.PeriodId
                       });

            if (res.Sum(f => f.PeriodId) != 39)
            {
                throw new Exception("Assert failure");
            }

#if (SERVER || SOCKETS)
            if(dispose) { Logic.Dispose(); }
#else
            if (dispose) { db.Dispose(); }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
#endif
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
