using RocksDbSharp;
using ServerSharedData;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public IDbQueryable<T> Search<T, TKey>(IDbQueryable<T> source, Expression<Func<T, TKey>> keySelector, string search_query, bool partial, bool timeLimited, int timeLimitInMs, int? start_step, int? steps)
        {
            if (start_step != null && steps == null || start_step == null && steps != null)
            {
                throw new LinqDbException("Linqdb: start_step and steps parameters must be used together.");
            }

            CheckTableInfo<T>(source.Partition);
            if (string.IsNullOrEmpty(search_query))
            {
                return source;
            }
            if (source.LDBTree == null)
            {
                source.LDBTree = new QueryTree();
            }
            var tree = source.LDBTree;
            if (tree.SearchInfo == null)
            {
                tree.SearchInfo = new List<SearchInfo>();
            }
            var info = new SearchInfo();
            tree.SearchInfo.Add(info);
            info.SearchQuery = search_query;
            info.Partial = partial;
            info.TimeLimited = timeLimited;
            info.SearchTimeInMs = timeLimitInMs;
            info.Start_step = start_step;
            info.Steps = steps;

            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector);
            var type_name = GetTypeName<T>(source.Partition);
            var table_info = GetTableInfo(type_name);
            info.TableInfo = table_info;
            info.Name = name;

            source.LDBTree.Prev = info;
            source.LDBTree.Prev.Id = source.LDBTree.Counter + 1;
            source.LDBTree.Counter++;

            return source;
        }

        public IDbQueryable<T> SearchBitfunnel<T, TKey>(IDbQueryable<T> source, Expression<Func<T, TKey>> keySelector, string search_query, int? stop_res_count = null)
        {
            CheckTableInfo<T>(source.Partition);
            if (string.IsNullOrEmpty(search_query))
            {
                return source;
            }
            if (source.LDBTree == null)
            {
                source.LDBTree = new QueryTree();
            }
            var tree = source.LDBTree;
            if (tree.SearchInfo == null)
            {
                tree.SearchInfo = new List<SearchInfo>();
            }
            var info = new SearchInfo();
            tree.SearchInfo.Add(info);
            info.SearchQuery = search_query;
            info.IsBitFunnel = true;
            info.Steps = stop_res_count;

            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector);
            var type_name = GetTypeName<T>(source.Partition);
            var table_info = GetTableInfo(type_name);
            info.TableInfo = table_info;
            info.Name = name;

            source.LDBTree.Prev = info;
            source.LDBTree.Prev.Id = source.LDBTree.Counter + 1;
            source.LDBTree.Counter++;

            return source;
        }

        public List<OperResult> Search(QueryTree tree, List<OperResult> oper_list, ReadOptions ro, out double? searchPercintile)
        {
            searchPercintile = null;
            if (tree.SearchInfo == null)
            {
                return oper_list;
            }
            
            foreach (var s in tree.SearchInfo)
            {
                double? onePercentile = null;
                var res = new List<int>();
                List<double> res_distances = null;
                if (s.IsBitFunnel)
                {
                    res = SearchOneBF(s, ro);
                }
                else if (s.IsVector)
                {
                    var vres = SearchOneVector(s, ro, (int)s.Steps);
                    res = vres.Select(f => f.Item1).ToList();
                    res_distances = vres.Select(f => f.Item2).ToList();
                }
                else
                {
                    res = SearchOne(s, ro, s.Partial, s.TimeLimited, s.SearchTimeInMs, out onePercentile, s.Steps, s.Start_step);
                }

                searchPercintile = onePercentile;
                var oper_res = new OperResult()
                {
                    All = false,
                    ResIds = res,
                    ResDistances = res_distances
                };
                oper_res.Id = s.Id;
                oper_res.OrWith = s.OrWith;
                oper_list.Add(oper_res);
            }

            return oper_list;
        }

        public List<int> SearchOne(SearchInfo info, ReadOptions ro, bool partial, bool timeLimited, int timeLimitInMs, out double? searchPercintile, int? steps = null, int? start_step = null)
        {
            searchPercintile = null;
            if (!info.Name.ToLower().EndsWith("search") && !info.Name.ToLower().EndsWith("searchs"))
            {
                throw new LinqDbException("Linqdb: only string properties named ...Search (or ...SearchS) are indexed and can be searched.");
            }
            int totalSteps = GetLastStep(info.TableInfo.Name);
            if (!timeLimited)
            {
                return MakeSearch(info.SearchQuery, info.TableInfo, info.Name, ro, partial, steps, start_step);
            }
            else
            {
                var sw = new Stopwatch();
                sw.Start();
                int oneSearchSteps = 5;
                var res = new List<int>();
                int stepCount = 0;
                for (; sw.ElapsedMilliseconds < timeLimitInMs && stepCount <= totalSteps; stepCount += oneSearchSteps)
                {
                    res.AddRange(MakeSearch(info.SearchQuery, info.TableInfo, info.Name, ro, partial, oneSearchSteps, stepCount));
                }
                if (stepCount > totalSteps)
                {
                    searchPercintile = 100;
                }
                else
                {
                    searchPercintile = (double)(stepCount * 100) / (double)totalSteps;
                }
                return res;
            }
        }

        public List<Tuple<int, double>> SearchOneVector(SearchInfo info, ReadOptions ro, int k)
        {
            if (!info.Name.ToLower().EndsWith("l2") || info.TableInfo.Columns[info.Name] != LinqDbTypes.vector_)
            {
                throw new LinqDbException("Linqdb: only vector properties named ...L2 are indexed and can be searched.");
            }
            return MakeVectorSearch(info.Vector, k, info.TableInfo, info.Name, ro);
        }

        public List<int> SearchOneBF(SearchInfo info, ReadOptions ro)
        {
            List<int> res = new List<int>();
            var search_query = info.SearchQuery;
            if (string.IsNullOrEmpty(search_query))
            {
                return new List<int>();
            }

            var skey = MakeSnapshotKey(info.TableInfo.TableNumber, info.TableInfo.ColumnNumbers[info.Name]);
            var snapid = leveld_db.Get(skey, null, ro);
            if (snapid == null)
            {
                return null;
            }
            var snapshot_id = Encoding.UTF8.GetString(snapid);
            if (!indexes.ContainsKey(info.TableInfo.Name + "|" + info.Name + "|" + snapshot_id))
            {
                throw new LinqDbException($"Linqdb: bitfunnel index on {info.TableInfo.Name}.{info.Name} is not created.");
            }

            var index = indexes[info.TableInfo.Name + "|" + info.Name + "|" + snapshot_id];
            skey = MakeSnapshotKey(info.TableInfo.TableNumber, info.TableInfo.ColumnNumbers["Id"]);
            snapid = leveld_db.Get(skey, null, ro);
            var id_snapshot_id = Encoding.UTF8.GetString(snapid);
            var ids_index = indexes[info.TableInfo.Name + "|Id|" + id_snapshot_id];
            var bf_size = bitfunnel_sizes[$"{info.TableInfo.Name}|{info.Name}"];

            var words = SharedUtils.NormaliseString(search_query).Split().Where(f => !string.IsNullOrEmpty(f)).Distinct().ToList();
            BitArray _scratch = null;

            for (int i = 0; i < index.Parts.Count(); i++) 
            {
                var ids = ids_index.Parts[i].IntValues;
                var list = new List<BitArray>();
                foreach (var w in words)
                {
                    var word = SharedUtils.NormaliseString(w);

                    var h1 = SharedUtils.HashFnv1a(word, bf_size);
                    var h2 = SharedUtils.HashDjb2(word, bf_size);
                    var h3 = SharedUtils.HashSdbm(word, bf_size);

                    var bits1 = index.Parts[i].BitValues[h1];
                    var bits2 = index.Parts[i].BitValues[h2];
                    var bits3 = index.Parts[i].BitValues[h3];

                    list.Add(bits1);
                    list.Add(bits2);
                    list.Add(bits3);
                }

                int len = list[0].Length;

                if (_scratch == null || _scratch.Length != len)
                {
                    _scratch = new BitArray(len);
                }

                _scratch.SetAll(true);


                    for (int j = 0; j < list.Count; j++)
                    {
                        _scratch.And(list[j]);
                    }

                    for (int j = 0; j < _scratch.Length; j++)
                    {
                        if (_scratch[j])
                        {
                            res.Add(ids[j]);
                        }
                    }

                    if (info.Steps != null && res.Count() >= info.Steps)
                    {
                        return res;
                    }

            }

            return res;
        }
    }


    public class SearchInfo : BaseInfo
    {
        public string SearchQuery { get; set; }
        public bool IsBitFunnel { get; set; }
        public bool IsVector { get; set; }
        public bool Partial { get; set; }
        public bool TimeLimited { get; set; }
        public int SearchTimeInMs { get; set; }
        public TableInfo TableInfo { get; set; }
        public string Name { get; set; }
        public int? Start_step { get; set; }
        public int? Steps { get; set; }
        public float[] Vector { get; set; }
    }
}
