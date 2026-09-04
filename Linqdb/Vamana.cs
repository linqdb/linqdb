// VamanaIndex.cs (Classic .NET Framework 4.8)
// Session-scoped Vamana/DiskANN-style index with in-session caching and optimized pruning.
//
// • READS neighbors, vectors, and entry points from disk **once** per id/session (lazy).
// • Maintains ALL graph and entry-point mutations in-memory (this class does not write to disk).
// • On FinalizeSession(), returns a FlushPlan containing:
//      – Dictionary<int, List<int>> NeighborsByNode: COMPLETE adjacency lists
//        for only those nodes whose neighbors changed this session.
//      – List<int> EntryPoints: full entry-point set if changed this session; null otherwise.
//      – int RealVectorDelta: signed count of true vectors added(+) / removed(-) this session.
// • Supports Write sessions (Upsert = Insert or Update decided internally), Delete sessions, and Search sessions.
// • Search sessions perform disk-only traversal but use **read-through caches** to ensure
//   each vector/neighbor/entrypoint is read at most once per search session.
//
// Performance tweaks:
//   – Robust prune uses squared L2 distances with **early-abandon**.
//   – **Norm cache** provides a lower bound to skip many p–q comparisons.
//   – Optional **pairwise distance cache** (FIFO) reuses d²(p,q) across upserts.
//   – **Deletes are fully deferred**: edge removals happen immediately, but repairs & EP updates run in FinalizeSession().
//   – **EntryPoint policy upgrades**:
//        • Degree-based rotation: every N upserts, try swapping in a stronger (higher-degree) seed.
//        • Diversity bump on refill/trim: prefer EPs with lower neighborhood overlap.
//        • Dynamic target (auto by default): EP count scales with corpus size via a delegate.
//   – **Null-vector semantics (Write sessions)**:
//        • Upsert(id, null): if id exists → remove from index; if id is new → ignore.
//        • Upsert(id, non-null) when id previously null/non-existent → insert normally.
//
// Delegates include table/column so the caller can route storage by shard/column.
//
// Constructor delegate signatures:
//     getVectorNeighboursFromDisk(tableNumber, columnNumber, id, info)  -> List<int>
//     loadVector(tableNumber, columnNumber, id, info)                   -> float[]
//     getCurrentEntryPoints(tableNumber, columnNumber, info)            -> List<int>
//     getExactVectorCount(tableNumber, columnNumber, info)              -> long    (optional; for dynamic EP sizing)
//
// Usage (per session):
//   var idx = new VamanaIndex(
//       mode: VamanaIndex.Mode.Write | Mode.Delete | Mode.Search,
//       cfg: new VamanaIndex.Config(),
//       tableNumber: 7,
//       columnNumber: 2,
//       info: yourOptionalRoutingObject,
//       getVectorNeighboursFromDisk: (t,c,id,info) => ...,
//       loadVector: (t,c,id,info) => ...,
//       getCurrentEntryPoints: (t,c,info) => ...,
//       getExactVectorCount: (t,c,info) => myMetaStore.Count(t,c,info) // optional
//   );
//
//   // Write session:
//   idx.UpsertOne(id, vectorOrNull);   // call repeatedly
//   var plan = idx.FinalizeSession();  // plan.NeighborsByNode + plan.EntryPoints + plan.RealVectorDelta
//
//   // Delete session (same pattern: call DeleteOne repeatedly, finalize at end):
//   idx.DeleteOne(id);
//   idx.DeleteMany(new [] { id1, id2, ... });
//   var plan = idx.FinalizeSession();
//
//   // Search session (no finalize needed):
//   var ids = idx.Search(q, k);

using System;
using System.Collections;
using System.Collections.Generic;

namespace LinqDbInternal
{
    public sealed class VamanaIndex
    {
        // ------------------------- Public Config -------------------------

        public sealed class Config
        {
            // Graph / Vamana
            public int M = 32;               // max out-degree per node
            public float Tau = 1.2f;         // robust pruning parameter

            // Upsert candidate collection
            public int BeamInsert = 64;      // beam width for graph walk
            public int LmaxInsert = 4000;    // max expansions during candidate collection
            public int Lcand = 256;          // candidate pool size before robust prune
            public int RecentTouchCount = 8; // use most recent upserts as extra seeds

            // Delete/Update repair — (deletes are deferred to FinalizeSession)
            public int LLocalRepair = 256;   // local candidate pool size during repair
            public int RefillTarget = 24;    // try to keep degree >= this (<= M)

            // Search (disk-only)
            public int BeamSearch = 64;
            public int LmaxSearch = 4000;
            public int Rerank = 200;
            public int NumSeeds = 4;
            public bool EarlyStop = true;

            // Entry points set maintenance (dynamic target by default)
            public bool EntryPointsAuto = true;   // <— dynamic by default
            public int EntryPointsTarget = 64;    // used when Auto=false or delegate missing
            // Auto sizing: target = clamp(base + slope * log10(N), min, max)
            public int EntryPointsAutoMin = 32;
            public int EntryPointsAutoMax = 256;
            public int EntryPointsAutoBase = 16;
            public double EntryPointsAutoSlopePerLog10 = 8.0;

            // EP rotation & gating
            public int EntryPointRotateEvery = 1024; // check every N upserts (0=off)
            public int EntryPointMinDegree = 0;      // defaulted to M/2 in ctor if <=0

            // Promotions from write session
            public int MaxAddFromWriteSession = 8;

            // Pairwise distance cache (squared L2); 0 disables
            public int PairwiseCacheCapacity = 300000;
        }

        public enum Mode
        {
            Write = 1,  // Upsert allowed (Insert + Update blended)
            Delete = 2,
            Search = 3
        }

        // ------------------ Storage routing (shard info) ------------------

        private readonly int _tableNumber;
        private readonly short _columnNumber;
        private readonly object _info;

        // ------------------ Disk READ dependencies ------------------

        private readonly Func<int, short, int, object, List<int>> _getNbrsDisk;      // (table, column, id, info) -> neighbors
        private readonly Func<int, short, int, object, float[]> _loadVector;         // (table, column, id, info) -> vector
        private readonly Func<int, short, object, List<int>> _getCurrentEntryPoints; // (table, column, info) -> entry points
        private readonly Func<int, short, object, long> _getExactVectorCount;        // (table, column, info) -> exact N (optional)

        // --------------------------- Config & Mode ---------------------------

        private readonly Config _cfg;
        private readonly Mode _mode;

        // ------------------------- Session Caches (Write/Delete) ------------

        private readonly Dictionary<int, float[]> _vecCache = new Dictionary<int, float[]>();
        private readonly Dictionary<int, float> _normCache = new Dictionary<int, float>();
        private readonly Dictionary<int, bool> _existsOnDisk = new Dictionary<int, bool>();

        private readonly Dictionary<int, HashSet<int>> _nbrCache = new Dictionary<int, HashSet<int>>();
        private readonly Dictionary<int, List<int>> _nbrInitial = new Dictionary<int, List<int>>();
        private readonly HashSet<int> _dirtyNodes = new HashSet<int>();

        // Entry points: working set + initial snapshot
        private bool _epLoaded = false;
        private List<int> _entryPointsCache = new List<int>();    // working EP set (order preserved)
        private List<int> _entryPointsInitial = new List<int>();  // initial EP set (for change detection)
        private int _epAddsThisSession = 0;
        private bool _epDirty = false;                             // changed in this session (deletes/upserts/rotation/diversity)
        private readonly HashSet<int> _deletedForEP = new HashSet<int>(); // ids to remove from EPs

        // EP rotation bookkeeping
        private int _upsertsSinceLastEPRotate = 0;

        // Session-local bookkeeping
        private readonly LinkedList<int> _recentTouches = new LinkedList<int>(); // newest upserts (Write sessions)
        private readonly HashSet<int> _tombstoned = new HashSet<int>();          // nodes deleted in-session (Delete or write-side delete)
        private readonly HashSet<int> _needsRepair = new HashSet<int>();         // survivors needing repair (Delete)

        private readonly Random _rng = new Random(12345);

        // Pairwise distance cache (optional)
        private PairwiseCache _pairCache;

        // -------------------- Corpus size (for dynamic EPs) -----------------

        private long _approxCount = -1; // -1 = unknown

        // -------------------- Real vector delta for this session ------------

        private int _realVectorDelta = 0; // signed: +inserts, -deletes

        // ------------------------- Search-only read caches -------------------

        private readonly Dictionary<int, float[]> _searchVecCache = new Dictionary<int, float[]>();
        private readonly Dictionary<int, List<int>> _searchNbrCache = new Dictionary<int, List<int>>();
        private List<int> _searchEntryPoints = null;

        // ----------------------------- Ctor -----------------------------

        public VamanaIndex(
            Mode mode,
            Config cfg,
            int tableNumber,
            short columnNumber,
            object info,
            // disk reads
            Func<int, short, int, object, List<int>> getVectorNeighboursFromDisk,
            Func<int, short, int, object, float[]> loadVector,
            Func<int, short, object, List<int>> getCurrentEntryPoints,
            Func<int, short, object, long> getExactVectorCount)
        {
            _mode = mode;
            _cfg = cfg ?? new Config();

            _tableNumber = tableNumber;
            _columnNumber = columnNumber;
            _info = info;

            if (getVectorNeighboursFromDisk == null) throw new ArgumentNullException("getVectorNeighboursFromDisk");
            if (loadVector == null) throw new ArgumentNullException("loadVector");
            if (getCurrentEntryPoints == null) throw new ArgumentNullException("getCurrentEntryPoints");

            _getNbrsDisk = getVectorNeighboursFromDisk;
            _loadVector = loadVector;
            _getCurrentEntryPoints = getCurrentEntryPoints;
            _getExactVectorCount = getExactVectorCount;

            if (_cfg.RefillTarget > _cfg.M) _cfg.RefillTarget = _cfg.M;
            if (_cfg.RefillTarget < 0) _cfg.RefillTarget = 0;
            if (_cfg.Rerank < 1) _cfg.Rerank = 1;

            // Default EntryPointMinDegree to M/2 if not set
            if (_cfg.EntryPointMinDegree <= 0 || _cfg.EntryPointMinDegree > _cfg.M)
                _cfg.EntryPointMinDegree = Math.Max(1, _cfg.M / 2);

            _pairCache = new PairwiseCache(_cfg.PairwiseCacheCapacity);

            // Initialize corpus size if auto-EPs enabled and delegate is available
            if (_cfg.EntryPointsAuto && _getExactVectorCount != null)
            {
                try { _approxCount = Math.Max(0, _getExactVectorCount(_tableNumber, _columnNumber, _info)); }
                catch { _approxCount = -1; }
            }
        }

        // =========================================================
        // =================== PUBLIC: UPSERT ONE ==================
        // =========================================================

        // Upsert semantics with null handling:
        //   - newVector == null:
        //       • if id exists in index -> remove it (write-side delete)
        //       • else -> ignore (no-op)
        //   - newVector != null:
        //       • normal insert or update depending on existence
        public void UpsertOne(int id, float[] newVector)
        {
            if (_mode != Mode.Write) throw new InvalidOperationException("This session is not in Write mode.");

            bool existed = ExistsOnDisk(id) || _nbrCache.ContainsKey(id);

            // === Null-vector handling ===
            if (newVector == null)
            {
                if (!existed) return; // brand-new + null => ignore
                RemoveFromGraphInternal(id); // existing + null => delete from index
                if (_approxCount > 0) _approxCount--;
                _realVectorDelta--; // authoritative real-vector delta
                return;
            }

            // Cache/refresh the vector
            _vecCache[id] = newVector;
            _normCache.Remove(id);

            var ctx = new UpsertContext(this, newVector);

            if (!existed)
            {
                // INSERT path
                var cand = CollectCandidatesForInsert(id, ctx, _cfg.Lcand, _cfg.BeamInsert, _cfg.LmaxInsert);
                var nx = RobustPrunePrecomputed(center: id, candidates: cand, M: _cfg.M, tau: _cfg.Tau, dCenterSq: ctx.Dx);

                for (int i = 0; i < nx.Count; i++) EnsureEdgeBidirectionalCached(id, nx[i]);

                for (int i = 0; i < nx.Count; i++) PruneNeighborhoodInPlaceCached(nx[i], _cfg.M, _cfg.Tau);
                PruneNeighborhoodInPlaceCached(id, _cfg.M, _cfg.Tau);

                Touch(id);
                TryPromoteEntryPointOnInsert(id); // may add to EP cache immediately

                if (_approxCount >= 0) _approxCount++; // track corpus growth if known
                _realVectorDelta++;                    // authoritative real-vector delta
            }
            else
            {
                // UPDATE path
                var cand = CollectCandidatesForUpdate(id, ctx, _cfg.Lcand);
                var newNbrs = RobustPrunePrecomputed(center: id, candidates: cand, M: _cfg.M, tau: _cfg.Tau, dCenterSq: ctx.Dx);

                var cur = GetNeighborsCached(id);
                var keep = new HashSet<int>(newNbrs);
                var touched = new HashSet<int>();

                for (int i = 0; i < cur.Count; i++)
                {
                    int v = cur[i];
                    touched.Add(v);
                    if (!keep.Contains(v)) RemoveEdgeBidirectionalCached(id, v);
                }
                for (int i = 0; i < newNbrs.Count; i++)
                {
                    int v = newNbrs[i];
                    touched.Add(v);
                    EnsureEdgeBidirectionalCached(id, v);
                }

                foreach (var u in touched) PruneNeighborhoodInPlaceCached(u, _cfg.M, _cfg.Tau);
                PruneNeighborhoodInPlaceCached(id, _cfg.M, _cfg.Tau);

                foreach (var u in touched)
                {
                    var Nu = GetNeighborsCached(u);
                    if (Nu.Count < _cfg.RefillTarget) RepairLocally(u);
                }

                Touch(id);
                // EPs unchanged for update (rotation still may happen below)
            }

            // EP rotation: every N upserts, try a cheap swap-in if candidate is strong enough
            _upsertsSinceLastEPRotate++;
            if (_cfg.EntryPointRotateEvery > 0 && _upsertsSinceLastEPRotate >= _cfg.EntryPointRotateEvery)
            {
                _upsertsSinceLastEPRotate = 0;
                EnsureEntryPointsLoaded();
                TryRotateEntryPointsByDegree(id);
            }
        }

        // =========================================================
        // =================== PUBLIC: DELETE ======================
        // =========================================================

        public void DeleteOne(int id)
        {
            if (_mode != Mode.Delete) throw new InvalidOperationException("This session is not in Delete mode.");

            bool existed = ExistsOnDisk(id) || _nbrCache.ContainsKey(id);
            if (!existed) return;

            RemoveFromGraphInternal(id);

            if (_approxCount > 0) _approxCount--;
            _realVectorDelta--; // authoritative
        }

        public void DeleteMany(IList<int> ids)
        {
            if (_mode != Mode.Delete) throw new InvalidOperationException("This session is not in Delete mode.");
            if (ids == null || ids.Count == 0) return;

            // Only operate on ids that actually exist (on disk or already cached this session)
            var toDelete = new List<int>(ids.Count);
            for (int i = 0; i < ids.Count; i++)
            {
                int id = ids[i];
                if (ExistsOnDisk(id) || _nbrCache.ContainsKey(id))
                    toDelete.Add(id);
            }
            if (toDelete.Count == 0) return;

            var del = new HashSet<int>(toDelete);
            var neighborsByDeleted = new Dictionary<int, List<int>>(toDelete.Count);

            for (int i = 0; i < toDelete.Count; i++)
            {
                int id = toDelete[i];
                neighborsByDeleted[id] = GetNeighborsCached(id);
            }

            foreach (var kv in neighborsByDeleted)
            {
                int id = kv.Key;
                var nx = kv.Value;

                for (int i = 0; i < nx.Count; i++)
                {
                    int u = nx[i];
                    if (del.Contains(u)) continue;
                    RemoveEdgeBidirectionalCached(id, u);
                    _needsRepair.Add(u);
                }
            }

            for (int i = 0; i < toDelete.Count; i++)
            {
                int id = toDelete[i];
                _tombstoned.Add(id);
                MarkDirty(id);

                var set = GetOrCreateNeighborsSet(id);
                if (set.Count > 0) set.Clear();

                _vecCache.Remove(id);
                _normCache.Remove(id);
                _deletedForEP.Add(id);
            }
            _epDirty = _epDirty || _deletedForEP.Count > 0;

            if (_approxCount >= 0) _approxCount = Math.Max(0, _approxCount - toDelete.Count);
            _realVectorDelta -= toDelete.Count; // authoritative
        }

        // =========================================================
        // ======================= PUBLIC: SEARCH ==================
        // =========================================================

        // Disk-only search with **per-session read caches**:
        //   – Each vector id is loaded at most once per search session.
        //   – Each neighbor list id is read once per search session.
        //   – Entry points read once per search session.
        public List<Tuple<int, double>> Search(float[] q, int k)
        {
            if (_mode != Mode.Search) throw new InvalidOperationException("This session is not in Search mode.");
            if (q == null) throw new ArgumentNullException("q");
            if (k <= 0) return new List<Tuple<int, double>>(0);

            if (_getExactVectorCount != null &&
                _getExactVectorCount(_tableNumber, _columnNumber, _info) == 0)
            {
                return new List<Tuple<int, double>>();
            }

            int rerank = Math.Max(k, _cfg.Rerank);

            var visited = new HashSet<int>();
            var candidates = new HashSet<int>();
            var topK = new TopK(k);
            var heap = new MinHeap();

            // Seeds (once per session)
            var seeds = SEntryPoints();
            int used = 0;
            for (int i = 0; i < seeds.Count && used < _cfg.NumSeeds; i++)
            {
                int s = seeds[i];
                var vs = SVec(s);
                if (vs == null) continue;

                float ds2 = L2Sq(q, vs);
                heap.Enqueue(s, ds2);
                candidates.Add(s);
                topK.InsertIfBetter(s, ds2);
                used++;
            }

            int expansions = 0;

            while (heap.Count > 0 && expansions < _cfg.LmaxSearch)
            {
                int u; float du2;
                heap.TryDequeue(out u, out du2);
                if (!visited.Add(u)) continue;
                expansions++;

                if (_cfg.EarlyStop && topK.IsFull && du2 >= topK.WorstDistance && expansions > _cfg.BeamSearch)
                    break;

                var nbrs = SNeighbors(u);
                for (int i = 0; i < nbrs.Count; i++)
                {
                    int v = nbrs[i];
                    if (visited.Contains(v)) continue;
                    var vv = SVec(v);
                    if (vv == null) continue;
                    float dv2 = L2Sq(q, vv);
                    heap.Enqueue(v, dv2);
                    candidates.Add(v);
                    topK.InsertIfBetter(v, dv2);
                }
            }

            // Exact rerank of candidates using squared L2
            var ranked = new List<Tuple<int, float>>(candidates.Count);
            foreach (var id in candidates)
            {
                var v = SVec(id);
                if (v == null) continue;
                ranked.Add(Tuple.Create(id, L2Sq(q, v)));
            }
            ranked.Sort((a, b) => a.Item2.CompareTo(b.Item2));
            if (ranked.Count > rerank) ranked.RemoveRange(rerank, ranked.Count - rerank);
            if (ranked.Count > k) ranked.RemoveRange(k, ranked.Count - k);

            var result = new List<Tuple<int, double>>(ranked.Count);
            for (int i = 0; i < ranked.Count; i++)
                result.Add(Tuple.Create(ranked[i].Item1, (double)ranked[i].Item2));

            return result;
        }

        // =========================================================
        // =================== PUBLIC: FINALIZE ====================
        // =========================================================

        public FlushPlan FinalizeSession()
        {
            // One-time batched repair for all survivors affected by deletes
            if ((_mode == Mode.Delete || _mode == Mode.Write) && _needsRepair.Count > 0)
            {
                foreach (var u in _needsRepair)
                {
                    if (_tombstoned.Contains(u)) continue;
                    var Nu = GetNeighborsCached(u);
                    if (Nu.Count < _cfg.RefillTarget) RepairLocally(u);
                }
                _needsRepair.Clear();
            }

            // Entry-points: apply deletions, refill/trim to target, apply diversity bump
            if (_epLoaded || _epDirty)
            {
                EnsureEntryPointsLoaded();

                if (_deletedForEP.Count > 0)
                {
                    _entryPointsCache.RemoveAll(id => _deletedForEP.Contains(id));
                    _deletedForEP.Clear();
                    _epDirty = true;
                }

                // --- #2 fix: if EP set is empty, bootstrap a seed from any surviving node ---
                if (_entryPointsCache.Count == 0)
                {
                    int seed = PickAnySurvivorNode();
                    if (seed != -1)
                    {
                        _entryPointsCache.Add(seed);
                        _epDirty = true;
                    }
                }

                int target = EffectiveEntryPointsTarget();

                // Refill up to target (diversity-aware)
                if (_entryPointsCache.Count < target)
                {
                    int needed = target - _entryPointsCache.Count;
                    var refill = RefillEntryPointsWithDiversity(_entryPointsCache, needed);
                    for (int i = 0; i < refill.Count; i++)
                        if (!_entryPointsCache.Contains(refill[i])) _entryPointsCache.Add(refill[i]);
                    if (refill.Count > 0) _epDirty = true;
                }

                // Trim if above target (drop lowest-degree first; tie-break by redundancy)
                if (_entryPointsCache.Count > target)
                {
                    TrimEntryPointsToTarget(target);
                    _epDirty = true;
                }
            }

            // Build per-node adjacency output ONLY for nodes that changed
            var outAdj = new Dictionary<int, List<int>>();
            foreach (var id in _dirtyNodes)
            {
                var set = GetOrCreateNeighborsSet(id);
                var list = new List<int>(set);
                list.Sort();
                outAdj[id] = list;
            }

            // Entry points delta -> full list if changed else null
            List<int> outEP = null;
            if (_epLoaded)
            {
                if (!SequenceEqual(_entryPointsCache, _entryPointsInitial))
                {
                    outEP = new List<int>(_entryPointsCache);
                }
            }

            // Capture & reset real-vector delta for this session
            int delta = _realVectorDelta;
            _realVectorDelta = 0;

            return new FlushPlan(outAdj, outEP, delta);
        }

        // Optional: refresh corpus count between batches
        public void RefreshCorpusCountFromDelegate()
        {
            if (_getExactVectorCount == null) return;
            try { _approxCount = Math.Max(0, _getExactVectorCount(_tableNumber, _columnNumber, _info)); }
            catch { /* keep previous */ }
        }

        // =========================================================
        // =============== INSERT/UPDATE (Upsert) HELPERS ==========
        // =========================================================

        private void Touch(int id)
        {
            _recentTouches.AddLast(id);
            while (_recentTouches.Count > _cfg.RecentTouchCount) _recentTouches.RemoveFirst();
        }

        private bool ExistsOnDisk(int id)
        {
            bool ex;
            if (_existsOnDisk.TryGetValue(id, out ex)) return ex;
            var probe = _loadVector(_tableNumber, _columnNumber, id, _info);
            ex = (probe != null && probe.Length > 0);   // <-- only count non-empty vectors
            _existsOnDisk[id] = ex;
            return ex;
        }

        private sealed class UpsertContext
        {
            private readonly VamanaIndex _owner;
            private readonly float[] _x;
            private readonly Dictionary<int, float> _distToX = new Dictionary<int, float>(); // id -> L2^2(x, id)

            public UpsertContext(VamanaIndex owner, float[] x)
            {
                _owner = owner;
                _x = x;
            }

            public float Dx(int id2)
            {
                float d2;
                if (_distToX.TryGetValue(id2, out d2)) return d2;
                var v = _owner.V(id2);
                if (v == null) return float.PositiveInfinity;
                d2 = L2Sq(_x, v);
                _distToX[id2] = d2;
                return d2;
            }

            public float DxEarly(int id2, float cutoffSq)
            {
                float d2;
                if (_distToX.TryGetValue(id2, out d2)) return d2;
                var v = _owner.V(id2);
                if (v == null) return float.PositiveInfinity;
                d2 = L2SqEarly(_x, v, cutoffSq);
                _distToX[id2] = d2;
                return d2;
            }
        }

        private sealed class BoundedMaxHeap
        {
            private readonly int _cap;
            private readonly List<int> _ids = new List<int>();
            private readonly List<float> _vals = new List<float>();
            private readonly HashSet<int> _uniq = new HashSet<int>();

            public float WorstSq { get; private set; }

            public BoundedMaxHeap(int capacity)
            {
                _cap = Math.Max(1, capacity);
                WorstSq = float.PositiveInfinity;
            }

            public int Count { get { return _ids.Count; } }

            public bool TryAdd(int id, float distSq)
            {
                if (!_uniq.Add(id)) return false;

                if (_ids.Count < _cap)
                {
                    _ids.Add(id);
                    _vals.Add(distSq);
                    SiftUp(_ids.Count - 1);
                    if (_ids.Count == _cap) WorstSq = _vals[0];
                    return true;
                }

                if (distSq >= _vals[0]) { _uniq.Remove(id); return false; }

                _uniq.Remove(_ids[0]);
                _ids[0] = id;
                _vals[0] = distSq;
                SiftDown(0);
                _uniq.Add(id);
                WorstSq = _vals[0];
                return true;
            }

            public List<int> ToListUnordered()
            {
                return new List<int>(_ids);
            }

            private void SiftUp(int i)
            {
                while (i > 0)
                {
                    int p = (i - 1) >> 1;
                    if (_vals[i] <= _vals[p]) break;
                    Swap(i, p);
                    i = p;
                }
            }

            private void SiftDown(int i)
            {
                int n = _ids.Count;
                while (true)
                {
                    int l = (i << 1) + 1;
                    int r = l + 1;
                    int largest = i;
                    if (l < n && _vals[l] > _vals[largest]) largest = l;
                    if (r < n && _vals[r] > _vals[largest]) largest = r;
                    if (largest == i) break;
                    Swap(i, largest);
                    i = largest;
                }
            }

            private void Swap(int i, int j)
            {
                int ti = _ids[i]; _ids[i] = _ids[j]; _ids[j] = ti;
                float tv = _vals[i]; _vals[i] = _vals[j]; _vals[j] = tv;
            }
        }

        private List<int> CollectCandidatesForInsert(int id, UpsertContext ctx, int lcand, int beam, int lmax)
        {
            var visited = new HashSet<int>();
            var expandHeap = new MinHeap();
            var candHeap = new BoundedMaxHeap(lcand);

            EnsureEntryPointsLoaded();
            var seedsList = new List<int>(_entryPointsCache);
            for (var node = _recentTouches.First; node != null; node = node.Next)
                seedsList.Add(node.Value);

            for (int i = 0; i < seedsList.Count; i++)
            {
                int s = seedsList[i];
                if (s == id) continue;
                if (_tombstoned.Contains(s)) continue;
                float ds2 = ctx.Dx(s);
                expandHeap.Enqueue(s, ds2);
                candHeap.TryAdd(s, ds2);
            }

            int expansions = 0;
            while (expandHeap.Count > 0 && expansions < lmax)
            {
                int u; float du2;
                expandHeap.TryDequeue(out u, out du2);
                if (!visited.Add(u)) continue;
                expansions++;

                var nbrs = GetNeighborsCached(u);
                for (int i = 0; i < nbrs.Count; i++)
                {
                    int v = nbrs[i];
                    if (v == id || visited.Contains(v) || _tombstoned.Contains(v)) continue;

                    float cutoff = candHeap.Count >= lcand ? candHeap.WorstSq : float.PositiveInfinity;
                    float dv2 = (cutoff < float.PositiveInfinity) ? ctx.DxEarly(v, cutoff) : ctx.Dx(v);

                    expandHeap.Enqueue(v, dv2);
                    candHeap.TryAdd(v, dv2);
                }

                if (candHeap.Count >= lcand && expansions >= beam) break;
            }

            return candHeap.ToListUnordered();
        }

        private List<int> CollectCandidatesForUpdate(int id, UpsertContext ctx, int lcand)
        {
            var pool = new HashSet<int>();
            var Nu = GetNeighborsCached(id);

            for (int i = 0; i < Nu.Count; i++) if (!_tombstoned.Contains(Nu[i])) pool.Add(Nu[i]);

            for (int i = 0; i < Nu.Count && pool.Count < lcand; i++)
            {
                var Nv = GetNeighborsCached(Nu[i]);
                for (int j = 0; j < Nv.Count && pool.Count < lcand; j++)
                {
                    int w = Nv[j];
                    if (w == id || _tombstoned.Contains(w)) continue;
                    pool.Add(w);
                }
            }

            if (pool.Count < lcand)
            {
                var extra = CollectCandidatesForInsert(id, ctx, lcand, _cfg.BeamInsert, _cfg.LmaxInsert);
                for (int i = 0; i < extra.Count && pool.Count < lcand; i++) pool.Add(extra[i]);
            }

            return new List<int>(pool);
        }

        private List<int> RobustPrunePrecomputed(int center, IList<int> candidates, int M, float tau, Func<int, float> dCenterSq)
        {
            float tau2 = tau * tau;

            var tmp = new List<Tuple<int, float>>(candidates.Count);
            for (int i = 0; i < candidates.Count; i++)
            {
                int c = candidates[i];
                if (c == center || _tombstoned.Contains(c)) continue;
                float d2 = dCenterSq(c);
                if (float.IsInfinity(d2)) continue;
                tmp.Add(Tuple.Create(c, d2));
            }
            tmp.Sort((a, b) => a.Item2.CompareTo(b.Item2));

            var kept = new List<int>(Math.Min(M, tmp.Count));
            for (int i = 0; i < tmp.Count; i++)
            {
                int p = tmp[i].Item1;
                float dp2 = tmp[i].Item2;
                bool accept = true;

                var vp = V(p);
                float np = Norm(p);
                float cutoffSq = dp2 / tau2;

                for (int j = 0; j < kept.Count; j++)
                {
                    int q = kept[j];

                    float nq = Norm(q);
                    float diff = np - nq;
                    float lb2 = diff * diff;
                    if (lb2 >= cutoffSq) continue;

                    float cached;
                    if (_pairCache.TryGet(p, q, out cached))
                    {
                        if (dp2 > tau2 * cached) { accept = false; break; }
                        continue;
                    }

                    float dpq2 = L2SqEarly(vp, V(q), cutoffSq);
                    _pairCache.Put(p, q, dpq2);

                    if (dp2 > tau2 * dpq2) { accept = false; break; }
                }

                if (accept)
                {
                    kept.Add(p);
                    if (kept.Count == M) break;
                }
            }
            return kept;
        }

        private List<int> RobustPrune(int center, IEnumerable<int> candidates, int M, float tau)
        {
            float tau2 = tau * tau;
            var vc = V(center);

            var tmp = new List<Tuple<int, float>>();
            var seen = new HashSet<int>();

            foreach (var c in candidates)
            {
                if (c == center) continue;
                if (_tombstoned.Contains(c)) continue;
                if (!seen.Add(c)) continue;
                var v = V(c);
                if (v == null) continue;
                float d2 = L2Sq(vc, v);
                tmp.Add(Tuple.Create(c, d2));
            }
            tmp.Sort((a, b) => a.Item2.CompareTo(b.Item2));

            var kept = new List<int>(Math.Min(M, tmp.Count));
            for (int i = 0; i < tmp.Count; i++)
            {
                int p = tmp[i].Item1;
                float dp2 = tmp[i].Item2;
                bool accept = true;

                var vp = V(p);
                float np = Norm(p);
                float cutoffSq = dp2 / tau2;

                for (int j = 0; j < kept.Count; j++)
                {
                    int q = kept[j];

                    float diff = np - Norm(q);
                    float lb2 = diff * diff;
                    if (lb2 >= cutoffSq) continue;

                    float cached;
                    if (_pairCache.TryGet(p, q, out cached))
                    {
                        if (dp2 > tau2 * cached) { accept = false; break; }
                        continue;
                    }

                    float dpq2 = L2SqEarly(vp, V(q), cutoffSq);
                    _pairCache.Put(p, q, dpq2);

                    if (dp2 > tau2 * dpq2) { accept = false; break; }
                }

                if (accept)
                {
                    kept.Add(p);
                    if (kept.Count == M) break;
                }
            }
            return kept;
        }

        private void PruneNeighborhoodInPlaceCached(int u, int M, float tau)
        {
            var nu = GetNeighborsCached(u);
            if (nu.Count <= M) return;

            var pruned = RobustPrune(u, nu, M, tau);
            var keep = new HashSet<int>(pruned);

            for (int i = 0; i < nu.Count; i++)
            {
                int v = nu[i];
                if (!keep.Contains(v)) RemoveEdgeBidirectionalCached(u, v);
            }
            for (int i = 0; i < pruned.Count; i++)
                EnsureEdgeBidirectionalCached(u, pruned[i]);
        }

        // =========================================================
        // ===================== DELETE/REPAIR HELPERS =============
        // =========================================================

        // Internal delete used by both Delete sessions and write-side "update-to-null"
        private void RemoveFromGraphInternal(int id)
        {
            var nx = GetNeighborsCached(id);
            for (int i = 0; i < nx.Count; i++)
            {
                int u = nx[i];
                if (_tombstoned.Contains(u)) continue;
                RemoveEdgeBidirectionalCached(id, u);
                _needsRepair.Add(u);  // allow batch repair in FinalizeSession()
            }

            _tombstoned.Add(id);     // ensures it won't be re-linked accidentally this session
            MarkDirty(id);
            var set = GetOrCreateNeighborsSet(id);
            if (set.Count > 0) set.Clear();

            _vecCache.Remove(id);
            _normCache.Remove(id);

            _deletedForEP.Add(id);
            _epDirty = true;
        }

        private void RepairLocally(int u)
        {
            var Nu = GetNeighborsCached(u);
            var pool = new HashSet<int>();

            // Survivor neighbors
            for (int i = 0; i < Nu.Count; i++)
                if (!_tombstoned.Contains(Nu[i])) pool.Add(Nu[i]);

            // Neighbors-of-neighbors
            for (int i = 0; i < Nu.Count && pool.Count < _cfg.LLocalRepair; i++)
            {
                int v = Nu[i];
                var Nv = GetNeighborsCached(v);
                for (int j = 0; j < Nv.Count && pool.Count < _cfg.LLocalRepair; j++)
                {
                    int w = Nv[j];
                    if (w == u || _tombstoned.Contains(w)) continue;
                    pool.Add(w);
                }
            }

            // Prune and apply
            var repaired = RobustPrune(u, pool, _cfg.M, _cfg.Tau);
            var keep = new HashSet<int>(repaired);

            for (int i = 0; i < Nu.Count; i++)
            {
                int v = Nu[i];
                if (!keep.Contains(v)) RemoveEdgeBidirectionalCached(u, v);
            }

            for (int i = 0; i < repaired.Count; i++)
                EnsureEdgeBidirectionalCached(u, repaired[i]);

            // Optional: try to keep degree >= RefillTarget via one-hop expansion
            if (repaired.Count < _cfg.RefillTarget)
            {
                var extras = new HashSet<int>(repaired);
                for (int i = 0; i < repaired.Count && extras.Count < _cfg.LLocalRepair; i++)
                {
                    int v = repaired[i];
                    var Nv = GetNeighborsCached(v);
                    for (int j = 0; j < Nv.Count && extras.Count < _cfg.LLocalRepair; j++)
                    {
                        int w = Nv[j];
                        if (w == u || _tombstoned.Contains(w)) continue;
                        extras.Add(w);
                    }
                }

                var topped = RobustPrune(u, extras, _cfg.M, _cfg.Tau);
                for (int i = 0; i < topped.Count; i++)
                    EnsureEdgeBidirectionalCached(u, topped[i]);
            }
        }

        // =========================================================
        // ============== ENTRY POINTS (EP) IMPROVEMENTS ===========
        // =========================================================

        private void TryPromoteEntryPointOnInsert(int id)
        {
            EnsureEntryPointsLoaded();

            int target = EffectiveEntryPointsTarget();
            if (_entryPointsCache.Count >= target) return;
            if (_epAddsThisSession >= _cfg.MaxAddFromWriteSession) return;
            if (_entryPointsCache.Contains(id)) return;

            _entryPointsCache.Add(id);
            _epAddsThisSession++;
            _epDirty = true;
        }

        // Degree-based rotation: swap weakest EP with candidate if candidate has higher degree and passes gate
        private void TryRotateEntryPointsByDegree(int candidateId)
        {
            if (_entryPointsCache.Count == 0) return;
            if (_tombstoned.Contains(candidateId)) return;

            var candDeg = GetNeighborsCached(candidateId).Count;
            if (candDeg < _cfg.EntryPointMinDegree) return;
            if (_entryPointsCache.Contains(candidateId)) return;

            // Find weakest (min-degree) current EP that is not tombstoned
            int weakestIdx = -1;
            int weakestDeg = int.MaxValue;

            for (int i = 0; i < _entryPointsCache.Count; i++)
            {
                int ep = _entryPointsCache[i];
                if (_tombstoned.Contains(ep)) { weakestIdx = i; weakestDeg = -1; break; }
                int d = GetNeighborsCached(ep).Count;
                if (d < weakestDeg) { weakestDeg = d; weakestIdx = i; }
            }

            if (weakestIdx < 0) return;
            if (candDeg <= weakestDeg) return; // no improvement

            _entryPointsCache[weakestIdx] = candidateId;
            _epDirty = true;
        }

        // Diversity-aware refill: prefer nodes with low neighborhood overlap to existing EPs
        private List<int> RefillEntryPointsWithDiversity(List<int> currentSeeds, int needed)
        {
            var outList = new List<int>(needed);
            var seen = new HashSet<int>(currentSeeds);

            // Candidate pool: neighbors of EPs + their neighbors + recent touches
            var pool = new HashSet<int>();
            for (int sIdx = 0; sIdx < currentSeeds.Count; sIdx++)
            {
                int s = currentSeeds[sIdx];
                var n1 = GetNeighborsCached(s);
                for (int i = 0; i < n1.Count; i++) if (!_tombstoned.Contains(n1[i])) pool.Add(n1[i]);

                for (int i = 0; i < n1.Count; i++)
                {
                    var n2 = GetNeighborsCached(n1[i]);
                    for (int j = 0; j < n2.Count; j++) if (!_tombstoned.Contains(n2[j])) pool.Add(n2[j]);
                }
            }
            for (var node = _recentTouches.First; node != null; node = node.Next) pool.Add(node.Value);

            // Shuffle pool for randomness
            var list = new List<int>(pool);
            Shuffle(list);

            // Precompute neighbor sets for current EPs
            var epNbr = new Dictionary<int, HashSet<int>>();
            for (int i = 0; i < currentSeeds.Count; i++)
                epNbr[currentSeeds[i]] = new HashSet<int>(GetNeighborsCached(currentSeeds[i]));

            // Greedy selection by low max-Jaccard to existing EPs
            for (int i = 0; i < list.Count && outList.Count < needed; i++)
            {
                int cand = list[i];
                if (seen.Contains(cand) || _tombstoned.Contains(cand)) continue;

                var Nc = new HashSet<int>(GetNeighborsCached(cand));
                if (Nc.Count < _cfg.EntryPointMinDegree) continue;

                float maxJac = 0f;
                foreach (var ep in currentSeeds)
                {
                    var Ne = epNbr[ep];
                    int inter = 0;
                    foreach (var v in Nc) if (Ne.Contains(v)) inter++;
                    int uni = Ne.Count + Nc.Count - inter;
                    float jac = (uni == 0) ? 0f : (float)inter / (float)uni;
                    if (jac > maxJac) maxJac = jac;
                    if (maxJac > 0.6f) break; // threshold for "too similar"
                }

                if (maxJac <= 0.6f)
                {
                    outList.Add(cand);
                    currentSeeds.Add(cand);
                    epNbr[cand] = Nc; // extend set for next picks
                    seen.Add(cand);
                }
            }

            return outList;
        }

        private void TrimEntryPointsToTarget(int target)
        {
            if (_entryPointsCache.Count <= target) return;

            // Rank EPs by (degree asc, redundancy desc) and drop worst
            var scores = new List<Tuple<int, int, float>>(_entryPointsCache.Count); // (id, degree, redundancy)
            var epNbr = new Dictionary<int, HashSet<int>>();
            foreach (var ep in _entryPointsCache)
                epNbr[ep] = new HashSet<int>(GetNeighborsCached(ep));

            foreach (var ep in _entryPointsCache)
            {
                int deg = epNbr[ep].Count;
                float redund = 0f;
                foreach (var ep2 in _entryPointsCache)
                {
                    if (ep2 == ep) continue;
                    int inter = 0;
                    var N1 = epNbr[ep];
                    var N2 = epNbr[ep2];
                    foreach (var v in N1) if (N2.Contains(v)) inter++;
                    int uni = N1.Count + N2.Count - inter;
                    float jac = (uni == 0) ? 0f : (float)inter / (float)uni;
                    if (jac > redund) redund = jac;
                }
                scores.Add(Tuple.Create(ep, deg, redund));
            }

            // Sort: lowest degree first, then highest redundancy first
            scores.Sort((a, b) =>
            {
                int c = a.Item2.CompareTo(b.Item2);           // degree asc
                if (c != 0) return c;
                return -a.Item3.CompareTo(b.Item3);           // redundancy desc
            });

            int toDrop = _entryPointsCache.Count - target;
            var dropSet = new HashSet<int>();
            for (int i = 0; i < toDrop && i < scores.Count; i++) dropSet.Add(scores[i].Item1);

            _entryPointsCache.RemoveAll(x => dropSet.Contains(x));
        }

        private void EnsureEntryPointsLoaded()
        {
            if (_epLoaded) return;
            var cur = _getCurrentEntryPoints(_tableNumber, _columnNumber, _info) ?? new List<int>();
            _entryPointsCache = DedupeKeepOrder(cur);
            _entryPointsInitial = new List<int>(_entryPointsCache);
            _epLoaded = true;
        }

        private int EffectiveEntryPointsTarget()
        {
            if (!_cfg.EntryPointsAuto || _approxCount < 0) return _cfg.EntryPointsTarget;

            double n = Math.Max(10, (double)_approxCount);
            double t = _cfg.EntryPointsAutoBase + _cfg.EntryPointsAutoSlopePerLog10 * Math.Log10(n);
            int target = (int)Math.Round(t);
            if (target < _cfg.EntryPointsAutoMin) target = _cfg.EntryPointsAutoMin;
            if (target > _cfg.EntryPointsAutoMax) target = _cfg.EntryPointsAutoMax;
            return target;
        }

        // =========================================================
        // ===================== Cached Graph I/O ==================
        // =========================================================

        private List<int> GetNeighborsCached(int id)
        {
            if (_tombstoned.Contains(id)) return new List<int>();

            HashSet<int> set;
            if (!_nbrCache.TryGetValue(id, out set))
            {
                var baseList = _getNbrsDisk(_tableNumber, _columnNumber, id, _info) ?? new List<int>();
                var snap = new List<int>(baseList);
                _nbrInitial[id] = snap;
                set = new HashSet<int>(baseList);
                _nbrCache[id] = set;
            }
            return new List<int>(set);
        }

        private void EnsureEdgeBidirectionalCached(int a, int b)
        {
            if (a == b) return;
            if (_tombstoned.Contains(a) || _tombstoned.Contains(b)) return;

            var setA = GetOrCreateNeighborsSet(a);
            var setB = GetOrCreateNeighborsSet(b);

            bool addedA = setA.Add(b);
            bool addedB = setB.Add(a);

            if (addedA) MarkDirty(a);
            if (addedB) MarkDirty(b);
        }

        private void RemoveEdgeBidirectionalCached(int a, int b)
        {
            var setA = GetOrCreateNeighborsSet(a);
            var setB = GetOrCreateNeighborsSet(b);

            bool removedA = setA.Remove(b);
            bool removedB = setB.Remove(a);

            if (removedA) MarkDirty(a);
            if (removedB) MarkDirty(b);
        }

        private HashSet<int> GetOrCreateNeighborsSet(int id)
        {
            HashSet<int> set;
            if (_nbrCache.TryGetValue(id, out set)) return set;

            var baseList = _getNbrsDisk(_tableNumber, _columnNumber, id, _info) ?? new List<int>();
            var snap = new List<int>(baseList);
            _nbrInitial[id] = snap;

            set = new HashSet<int>(baseList);
            _nbrCache[id] = set;
            return set;
        }

        private void MarkDirty(int id)
        {
            _dirtyNodes.Add(id);
        }

        // =========================================================
        // ===================== Search read caches =================
        // =========================================================

        private float[] SVec(int id)
        {
            float[] v;
            if (_searchVecCache.TryGetValue(id, out v)) return v;
            v = _loadVector(_tableNumber, _columnNumber, id, _info);
            if (v == null || v.Length == 0) return null;  // <-- ignore empty
            _searchVecCache[id] = v;
            return v;
        }

        private List<int> SNeighbors(int id)
        {
            List<int> l;
            if (_searchNbrCache.TryGetValue(id, out l)) return l;
            l = _getNbrsDisk(_tableNumber, _columnNumber, id, _info) ?? new List<int>();
            _searchNbrCache[id] = l;
            return l;
        }

        private List<int> SEntryPoints()
        {
            if (_searchEntryPoints != null) return _searchEntryPoints;
            _searchEntryPoints = _getCurrentEntryPoints(_tableNumber, _columnNumber, _info) ?? new List<int>();
            return _searchEntryPoints;
        }

        // =========================================================
        // ======================== Utilities ======================
        // =========================================================

        // Pick any non-tombstoned node to bootstrap EPs if they become empty.
        // Preference order: keys seen in _nbrCache → keys seen in _nbrInitial → any neighbor of those
        private int PickAnySurvivorNode()
        {
            foreach (var kv in _nbrCache)
                if (!_tombstoned.Contains(kv.Key)) return kv.Key;

            foreach (var kv in _nbrInitial)
            {
                if (!_tombstoned.Contains(kv.Key)) return kv.Key;
                var lst = kv.Value;
                for (int i = 0; i < lst.Count; i++)
                {
                    int v = lst[i];
                    if (!_tombstoned.Contains(v)) return v;
                }
            }

            // As a last resort, if we ever had vectors in cache and they aren't tombstoned, pick one
            foreach (var kv in _vecCache)
                if (!_tombstoned.Contains(kv.Key)) return kv.Key;

            return -1; // none observed in this session
        }

        // Vector accessor (session-aware)
        private float[] V(int id)
        {
            float[] v;
            if (_vecCache.TryGetValue(id, out v)) return v;
            if (_tombstoned.Contains(id)) return null;
            v = _loadVector(_tableNumber, _columnNumber, id, _info);
            if (v == null || v.Length == 0) return null;  // <-- ignore empty
            _vecCache[id] = v;
            return v;
        }

        // Norm accessor (session-aware)
        private float Norm(int id)
        {
            float n;
            if (_normCache.TryGetValue(id, out n)) return n;
            var v = V(id);
            if (v == null) return float.PositiveInfinity;
            double s = 0.0;
            for (int i = 0; i < v.Length; i++) s += (double)v[i] * v[i];
            n = (float)Math.Sqrt(s);
            _normCache[id] = n;
            return n;
        }

        private static float L2Sq(float[] a, float[] b)
        {
            if (a == null || b == null) return float.PositiveInfinity;
            if (a.Length != b.Length)
                throw new LinqDbException(string.Format("Linqdb: vector dimensionality changed from {0} to {1}", b.Length, a.Length));

            double s = 0.0;
            int n = a.Length;
            for (int i = 0; i < n; i++)
            {
                double d = a[i] - b[i];
                s += d * d;
            }
            return (float)s;
        }

        private static float L2SqEarly(float[] a, float[] b, float cutoffSq)
        {
            if (a == null || b == null) return float.PositiveInfinity;
            if (a.Length != b.Length)
                throw new LinqDbException(string.Format("Linqdb: vector dimensionality changed from {0} to {1}", b.Length, a.Length));

            double s = 0.0;
            int n = a.Length;
            for (int i = 0; i < n; i++)
            {
                double d = a[i] - b[i];
                s += d * d;
                if (s > cutoffSq) return (float)s;
            }
            return (float)s;
        }

        private static float L2(float[] a, float[] b)
        {
            if (a == null || b == null) return float.PositiveInfinity;
            if (a.Length != b.Length)
                throw new LinqDbException(string.Format("Linqdb: vector dimensionality changed from {0} to {1}", b.Length, a.Length));
            return (float)Math.Sqrt(L2Sq(a, b));
        }

        private void Shuffle(IList<int> a)
        {
            for (int i = a.Count - 1; i > 0; i--)
            {
                int j = _rng.Next(i + 1);
                int t = a[i]; a[i] = a[j]; a[j] = t;
            }
        }

        private static List<int> DedupeKeepOrder(IEnumerable<int> seq)
        {
            var seen = new HashSet<int>();
            var outList = new List<int>();
            foreach (var x in (seq ?? new List<int>()))
                if (seen.Add(x)) outList.Add(x);
            return outList;
        }

        private static bool SequenceEqual(List<int> a, List<int> b)
        {
            if (ReferenceEquals(a, b)) return true;
            if (a == null || b == null) return false;
            if (a.Count != b.Count) return false;
            for (int i = 0; i < a.Count; i++)
                if (a[i] != b[i]) return false;
            return true;
        }

        // ------------------------ Small helpers ------------------------

        // Minimal min-heap keyed by float using SortedDictionary<float, Queue<int>> (ties via FIFO)
        private sealed class MinHeap
        {
            private readonly SortedDictionary<float, Queue<int>> _sd = new SortedDictionary<float, Queue<int>>();
            private int _count;

            public int Count { get { return _count; } }

            public void Enqueue(int node, float priority)
            {
                Queue<int> q;
                if (!_sd.TryGetValue(priority, out q))
                {
                    q = new Queue<int>();
                    _sd[priority] = q;
                }
                q.Enqueue(node);
                _count++;
            }

            public bool TryDequeue(out int node, out float priority)
            {
                if (_sd.Count == 0) { node = default(int); priority = default(float); return false; }
                var enumerator = _sd.GetEnumerator();
                enumerator.MoveNext();
                var key = enumerator.Current.Key;
                var q = enumerator.Current.Value;
                node = q.Dequeue();
                priority = key;
                if (q.Count == 0) _sd.Remove(key);
                _count--;
                return true;
            }
        }

        private sealed class TopK
        {
            private readonly int _cap;
            private readonly Dictionary<int, float> _map = new Dictionary<int, float>();
            public float WorstDistance { get; private set; }

            public bool IsFull { get { return _map.Count >= _cap; } }

            public TopK(int capacity)
            {
                _cap = Math.Max(1, capacity);
                WorstDistance = float.PositiveInfinity;
            }

            public void InsertIfBetter(int id, float distSq)
            {
                float existing;
                if (_map.TryGetValue(id, out existing))
                {
                    if (distSq < existing) { _map[id] = distSq; RecomputeWorst(); }
                    return;
                }
                if (_map.Count < _cap)
                {
                    _map[id] = distSq; RecomputeWorst(); return;
                }
                if (distSq < WorstDistance)
                {
                    int worstId = -1; float worst = float.NegativeInfinity;
                    foreach (var kv in _map)
                        if (kv.Value > worst) { worst = kv.Value; worstId = kv.Key; }
                    if (worstId != -1) _map.Remove(worstId);
                    _map[id] = distSq;
                    RecomputeWorst();
                }
            }

            private void RecomputeWorst()
            {
                float worst = float.NegativeInfinity;
                foreach (var kv in _map)
                    if (kv.Value > worst) worst = kv.Value;
                WorstDistance = (_map.Count == 0) ? float.PositiveInfinity : worst;
            }
        }

        private sealed class PairwiseCache
        {
            private readonly int _cap;
            private readonly Dictionary<long, float> _map = new Dictionary<long, float>();
            private readonly Queue<long> _fifo = new Queue<long>();

            public PairwiseCache(int cap) { _cap = Math.Max(0, cap); }

            private static long Key(int a, int b)
            {
                if (a > b) { int t = a; a = b; b = t; }
                return ((long)(uint)a << 32) | (uint)b;
            }

            public bool TryGet(int a, int b, out float d2)
            {
                if (_cap == 0) { d2 = 0f; return false; }
                return _map.TryGetValue(Key(a, b), out d2);
            }

            public void Put(int a, int b, float d2)
            {
                if (_cap == 0) return;
                long k = Key(a, b);
                if (_map.ContainsKey(k)) { _map[k] = d2; return; }
                if (_map.Count >= _cap) { var old = _fifo.Dequeue(); _map.Remove(old); }
                _map[k] = d2; _fifo.Enqueue(k);
            }
        }
    }

    // --------------------------- Flush Plan ----------------------------
    public sealed class FlushPlan
    {
        // For each CHANGED node, the COMPLETE neighbor list to persist.
        public readonly Dictionary<int, List<int>> NeighborsByNode;

        // Full entry-point list if it changed this session; null if unchanged or never used.
        public readonly List<int> EntryPoints;

        // Signed count of *real* vectors added(+) or removed(-) this session.
        public readonly int RealVectorDelta;

        public FlushPlan(Dictionary<int, List<int>> neighborsByNode, List<int> entryPoints, int realVectorDelta)
        {
            this.NeighborsByNode = neighborsByNode ?? new Dictionary<int, List<int>>();
            this.EntryPoints = entryPoints; // may be null to indicate "no change"
            this.RealVectorDelta = realVectorDelta;
        }
    }
}
