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
using System.Threading;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    class VectorSearchBigSmallDim : ITest
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
            db.Table<SomeVector>().DeleteNonAtomically(new HashSet<int>(db.Table<SomeVector>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));

            int total = 1000;
            //#if (DATA)
            //    total = 102000;
            //#endif

            var list = new List<SomeVector>();
            for (int i = 0; i < total; i++)
            {
                list.Add(new SomeVector()
                {
                    VectorL2 = VecOps.RandomVector(12)
                });
                if (list.Count() > 100)
                {
                    db.Table<SomeVector>().SaveBatch(list);
                    list = new List<SomeVector>();
                }
            }

            if (list.Count() > 0)
            {
                db.Table<SomeVector>().SaveBatch(list);
                list = new List<SomeVector>();
            }

            var count = db.Table<SomeVector>().Count();

            var vector1 = Enumerable.Range(0, 12).Select(f => (float)1).ToArray();
            var res1 = db.Table<SomeVector>().SearchNeighbours(f => f.VectorL2, vector1, 1).SelectEntity().Single().VectorL2;

            var vector2 = Enumerable.Range(0, 12).Select(f => (float)0).ToArray();
            var res2 = db.Table<SomeVector>().SearchNeighbours(f => f.VectorL2, vector2, 1).SelectEntity().Single().VectorL2;

            var d1 = VecOps.L2(vector1, res1);
            var d2 = VecOps.L2(vector2, res2);

            var d12 = VecOps.L2(vector1, res2);
            var d21 = VecOps.L2(vector2, res1);

            if (d1 > d12 || d1 > d21 || d2 > d12 || d2 > d21)
            {
                throw new Exception("Assert failure");
            }


            db.Table<SomeVector>().DeleteNonAtomically(new HashSet<int>(db.Table<SomeVector>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));

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

    public static class VecOps
    {
        /// <summary>
        /// Returns a float[] of length n with components uniformly sampled in [0, 1).
        /// Pass your own Random for reproducibility; otherwise a thread-local RNG is used.
        /// </summary>
        public static float[] RandomVector(int n, Random rng = null)
        {
            if (n < 0) throw new ArgumentOutOfRangeException("n");
            var r = rng ?? _tlsRandom.Value;
            var v = new float[n];
            for (int i = 0; i < n; i++)
                v[i] = (float)r.NextDouble(); // in [0,1)
            return v;
        }

        /// <summary>
        /// Euclidean (L2) distance between two equal-length float vectors.
        /// </summary>
        public static float L2(float[] a, float[] b)
        {
            if (a == null) throw new ArgumentNullException("a");
            if (b == null) throw new ArgumentNullException("b");
            if (a.Length != b.Length) throw new ArgumentException("Vectors must have the same length.");

            double sum = 0.0;
            for (int i = 0; i < a.Length; i++)
            {
                double d = a[i] - b[i];
                sum += d * d;
            }
            return (float)Math.Sqrt(sum);
        }

        // Thread-local RNG to avoid repeat sequences if RandomVector is called rapidly without a provided RNG.
        private static readonly ThreadLocal<Random> _tlsRandom =
            new ThreadLocal<Random>(() => new Random(unchecked(Environment.TickCount * 31 + Thread.CurrentThread.ManagedThreadId)));
    }
}
