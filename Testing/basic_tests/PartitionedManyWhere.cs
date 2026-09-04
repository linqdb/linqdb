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
using System.Threading;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    class PartitionedManyWhere : ITest
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
            
            ThreadPool.QueueUserWorkItem(q => 
            {
                APartitioned.Insert(db);
            });
            Thread.Sleep(3000);


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

    public class APartitioned
    {
        static string partition = "1";
        public static void Insert(Db db)
        {
            try
            {
                var norm = 1.2;
                int a = 5;
                int period = a == 5 ? 5 : 6;
                var date = DateTime.Now.AddDays(-1);
                var name = "test";
                var d = new SomeData()
                {
                    Id = 1,
                    Normalized = norm,
                    PeriodId = period,
                    Date = date,
                    NameSearch = name
                };

                db.PartitionedTable<SomeData>(partition)
                  .Where(f => f.PeriodId == period && f.Normalized == norm && f.Date == date && f.NameSearch == name)
                  .AtomicIncrement2Props(f => f.PeriodId, f => f.PersonId, 1, 1, d);

            }
            catch (Exception)
            {
                throw new Exception("Assert failure");
            }
        }
    }
}
