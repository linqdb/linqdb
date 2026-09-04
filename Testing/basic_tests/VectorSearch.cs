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
    class VectorSearch : ITest
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
                VectorL2 = new float[] { 0.01f, 0.02f, 0.03f }
            });

            db.Table<SomeVector>().Save(new SomeVector()
            {
                Id = 2,
                Date = DateTime.Now,
                VectorL2 = new float[] { 0.011f, 0.022f, 0.033f }
            });

            db.Table<SomeVector>().Save(new SomeVector()
            {
                Id = 3,
                Date = DateTime.Now,
                VectorL2 = new float[] { -0.01f, -0.02f, -0.03f }
            });

            db.Table<SomeVector>().Save(new SomeVector()
            {
                Id = 4,
                Date = DateTime.Now,
                VectorL2 = new float[] { -0.011f, -0.022f, -0.033f }
            });

            var res = db.Table<SomeVector>()
                        .SearchNeighbours(f => f.VectorL2, new float[3] { -0.015f, -0.025f, -0.035f }, 1)
                        .SelectEntity();

            if (res.Count() != 1 || res[0].Id != 4)
            {
                throw new Exception("Assert failure");
            }


            db.Table<SomeVector>().Delete(4);

            res = db.Table<SomeVector>()
                        .SearchNeighbours(f => f.VectorL2, new float[3] { -0.015f, -0.025f, -0.035f }, 1)
                        .SelectEntity();

            if (res.Count() != 1 || res[0].Id != 3)
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
