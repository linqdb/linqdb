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
using Testing.distributedtables;
using Testing.tables;

namespace Testing.basic_tests
{
    public class SimpleSaveSid : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false;
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
            db.Table<SomeDataDistributed>().Delete(new HashSet<int>(db.Table<SomeDataDistributed>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif

            var d = new SomeDataDistributed()
            {
                Id = 1,
                Normalized = 1.2,
                PeriodId = 5
            };
            try
            {
                db.Table<SomeDataDistributed>().Save(d);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("Sid"))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                db.Table<SomeDataDistributed>().SaveBatch(new List<SomeDataDistributed>() { d });
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("Sid"))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                db.Table<SomeDataDistributed>().SaveNonAtomically(new List<SomeDataDistributed>() { d });
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("Sid"))
                {
                    throw new Exception("Assert failure");
                }
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
