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
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    class UpdateNonNullabeIntNull : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false; if (db == null) { db = new Db("DATA"); dispose = true; }
#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif
#if (SOCKETS || SAMEDB || INDEXES || SERVER)
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
            var d = new SomeData()
            {
                Id = 1,
                PeriodId = 2,
                Normalized = 1
            };
            db.Table<SomeData>().Save(d);

            db.Table<SomeData>().Update(f => f.PeriodId, new Dictionary<int, int>() { { 1, 2 } });
            db.Table<SomeData>().Update(f => f.ObjectId, new Dictionary<int, int?>() { { 1, null } });

            try
            {
                db.Table<SomeData>().Update(f => f.PeriodId, new Dictionary<int, int?>() { { 1, null } });
                throw new Exception("Assert failure");
            }
            catch (Exception ex) 
            {
                if (!ex.Message.Contains("Non-nullable type is updated with null value"))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                db.Table<SomeData>().UpdateNonAtomically(f => f.PeriodId, new Dictionary<int, int?>() { { 1, null } });
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("Non-nullable type is updated with null value"))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                db.PartitionedTable<SomeData>("abc").Update(f => f.PeriodId, new Dictionary<int, int?>() { { 1, null } });
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("Non-nullable type is updated with null value"))
                {
                    throw new Exception("Assert failure");
                }
            }


            var res = db.Table<SomeData>().SelectEntity();
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
