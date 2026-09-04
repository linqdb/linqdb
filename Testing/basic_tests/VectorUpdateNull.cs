#if (SERVER || SOCKETS)
using LinqdbClient;
using ServerLogic;
#else
using LinqDb;
#endif
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    class VectorUpdateNull : ITest
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
            db.Table<SomeVector>().Delete(new HashSet<int>(db.Table<SomeVector>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));


            db.Table<SomeVector>().Save(new SomeVector()
            { 
                Id = 1,
                Date = DateTime.Now,
                Value = 5.5555m,
                VectorL2 = new float[] { 0.01f, 0.02f, 0.03f }
            });


            var stat = new LinqdbSelectStatistics();
            var sres = db.Table<SomeVector>().SearchNeighbours(f => f.VectorL2, new float[] { 0.01f, 0.02f, 0.03f }, 1).SelectEntity(stat);

            if (sres.Count() != 1 || sres[0].Id != 1)
            {
                throw new Exception("Assert failure");
            }

            if (stat.Distances.Single().Value != 0)
            {
                throw new Exception("Assert failure");
            }


            var res = db.Table<SomeVector>()
                        .SelectEntity();

            if (res.Count() != 1 || res[0].VectorL2.Sum() != 0.06f)
            {
                throw new Exception("Assert failure");
            }

            res = db.Table<SomeVector>()
                    .Where(f => f.VectorL2 == null)
                    .SelectEntity();

            if (res.Count() != 0)
            {
                throw new Exception("Assert failure");
            }

            res = db.Table<SomeVector>()
                    .Where(f => f.Value == null)
                    .SelectEntity();

            if (res.Count() != 0)
            {
                throw new Exception("Assert failure");
            }

            db.Table<SomeVector>().Update(f => f.VectorL2, new Dictionary<int, float[]>() { { 1, null } });

            res = db.Table<SomeVector>()
                    .Where(f => f.VectorL2 == null)
                    .SelectEntity();

            if (res.Count() != 1)
            {
                throw new Exception("Assert failure");
            }

            sres = db.Table<SomeVector>().SearchNeighbours(f => f.VectorL2, new float[] { 0.01f, 0.02f, 0.03f }, 1).SelectEntity(stat);

            if (sres.Count() != 0)
            {
                throw new Exception("Assert failure");
            }

            db.Table<SomeVector>().Update(f => f.Value, new Dictionary<int, decimal?>() { { 1, null } });

            res = db.Table<SomeVector>()
                    .Where(f => f.Value == null)
                    .SelectEntity();

            if (res.Count() != 1)
            {
                throw new Exception("Assert failure");
            }

            db.Table<SomeVector>().Update(f => f.VectorL2, new Dictionary<int, float[]>() { { 1, null } });

            res = db.Table<SomeVector>()
                    .Where(f => f.VectorL2 == null)
                    .SelectEntity();

            if (res.Count() != 1)
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
