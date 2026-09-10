### What is Linqdb


&#x20;   Technically it's just a wrapper around Rocksdb. It has many&#x20;
simillarities to relational databases, namely tables, sql-style "select&#x20;
from where" thinking.
&#x20;   So why use it and when? 

&#x20;   Linqdb is OLTP database with focus on rapid development.&#x20;
A simplest and easiest use-case is embedded db as an alternative to sqlite.&#x20;
However, right now it still needs work to become real ACID OLTP db:

-
  &#x20;              Embedded db needs transaction support for partitioned tables.
  &#x20;           
-
  &#x20;              Embedded db needs a way to read from more than one table within same data snapshot.
  &#x20;           
-
  &#x20;              Single server case needs all of the above too.
  &#x20;
-
  &#x20;              Distributed db needs support for distributed transactions and all of the above.
  &#x20;               


### Creating new entities

An entity or a table or a collection is defined by a class. The class
&#x20;must have int Id property which if 0 indicates new entity and if not -&#x20;
the entity to be updated. Supported data types are: `int`, `double`, `DateTime`, `long`, `decimal` (and their nullables), `byte[]` and `string`. (Note `bool` is not supported, use `int` instead). For example, we’ll use:

```
public class SomeData
{
    public int Id { get; set; }
    public int PeriodId { get; set; }
    public int? ObjectId { get; set; }
    public double Value { get; set; }
    public double? Normalized { get; set; }
    public int? PersonId { get; set; }
    public DateTime? Date { get; set; }
    public string Name { get; set; }
    public string NameSearch { get; set; }
    public string StringVal { get; set; }
}

```

Then to save new record we would (table will be created automatically if not there based on type's name):

```

using LinqdbClient;
public class DbFactory
{
    static object _lock = new object();
    static Db _db { get; set; }

    public static Db GetDb()
    {
        if (_db == null)
        {
            lock (_lock)
            {
                if (_db == null)
                {
                    _db = new Db("ip:port", "user", "pass");
                }
            }
        }

        return _db;
    }
}

```

```

...
var db = DbFactory.GetDb()

var d = new SomeData()
{
    Id = 0,
    Normalized = 1.2,
    PeriodId = 5
};
db.Table<SomeData>().Save(d);

//db.Dispose(); //disposal is not needed after an operation, only (optionally) at the end of application lifetime.

```

To save bulk of data more efficiently:

```
var list = new List<SomeData>();
...
db.Table<SomeData>().SaveBatch(list);

```

### Querying

**Select**

Select everything from table:

```

var db = DbFactory.GetDb();

List<SomeData> res = db.Table<SomeData>().SelectEntity();

```

To select only some columns:

```
var res = db.Table<SomeData>()
            .Select(f => new 
            { 
                Id = f.Id,
                Normalized = f.Normalized
            });


```

Note that only anonymous types are supported in this case. res is `List<AnonymousType>`


&#x20;   There is a limit in the amount of data that can be selected at once.
&#x20;If you need to select large amount there are non-atomic selects which&#x20;
would do the work in iterations.
&#x20;   The drawback of this method is the data may change between&#x20;
iterations and the result might be mismatching initial select&#x20;
conditions.


**Where**

Linqdb supports `Where` clause:

```
var res = db.Table<SomeData>()
            .Where(f => f.Normalized == 2.3 || f.Date > DateTime.Now && f.PersonId == 5)
            .Select(f => new 
            { 
                Id = f.Id,
                Normalized = f.Normalized
            });

```


&#x20;   Where supports these operators: `&& || == >= > < <= !=`
&#x20;. Also on the left hand side of operator there must be some property&#x20;
without any expression (i.e. f.PersonId % 2 == 0 won‘t work), on the&#x20;
right hand side - constant/variable/expression.
&#x20;   Types on both sides must match exactly (i.e. int != int?) and&#x20;
casting is only possible on the right side. 

&#x20;   Empty string and empty array is treated as null.


**Between**


&#x20;   To evaluate something like .Where(f => f.Normalized > 3&#x20;
&& f.Normalized < 10) Linqdb will iterate data twice: first&#x20;
it will find all values > 3, then all values < 10 and finally&#x20;
return it’s intersection.
&#x20;   So in this case it’s faster to use .Between(f => f.Normalized, 3,
&#x20;10, BetweenBoundaries.BothExclusive) which will only scan data once:&#x20;
from value 3 to 10.


```
var res = db.Table<SomeData>()
            .Between(f => f.Normalized, 3, 10, BetweenBoundaries.BothExclusive)
            .Select(f => new 
            { 
                Id = f.Id,
                Normalized = f.Normalized
            });

```

**Intersect**

Finds intersection of values in table with given set:

```
var res = db.Table<SomeData>()
            .Intersect(f => f.Normalized, new HashSet<double>() { 10, 20, 30 })
            .Select(f => new
            {
                Id = f.Id,
                Normalized = f.Normalized
            });

```

**Order, skip, take**

Ordering (by one column) is supported:

```
var res = db.Table<SomeData>()
            .OrderBy(f => f.Normalized)
            .Skip(10)
            .Take(10)
            .Select(f => new
            {
                Id = f.Id,
                Normalized = f.Normalized
            });

```


&#x20;   Ordering by `Id` is the fastest.



&#x20;   There are also overloads of `Select` and `SelectEntity` that take object of type `LinqdbSelectStatistics`. This object will be populated with the number of records that satisfied condition.
&#x20;   (Handy with `.Skip` and `.Take` when the total number is also needed). `SearchedPercentile` shows how much `SearchTimeLimited` has managed to search in given time.


**Search**

Linqdb supports simple full text search on string columns that end with ..Search (or .SearchS). For example,

```
var res = db.Table<SomeData>()
            .Search(f => f.NameSearch, "some text")
            .Select(f => new
            {
                Id = f.Id,
                Name = f.NameSearch
            });

```


&#x20;   will find rows where column `NameSearch` contains both "some" and "text". 

&#x20;   `.Search` takes optional prameters `start_step` and `steps`
&#x20;which enable search on some part of documents.
&#x20;   This is handy when you have lots of data and want to return some&#x20;
results as fast as possible.
&#x20;   The search will only happen on slice of data, for example,&#x20;
start\_step = 0, steps = 1 means only first 1000 documents will be&#x20;
searched, start\_step = 10, steps = 5 - 5000 documents will be searched&#x20;
starting from document 10000.
&#x20;   You can get total number of steps using `LastStep` function.



&#x20;   You can also limit search time by using `SearchTimeLimited`
&#x20;\- it will search as much as it can in given time (how much was searched
&#x20;is in LinqdbSelectStatistics object's SearchedPercentile property).



&#x20;   You can make search by the start of word by using `SearchPartial`. In this case, however, you cannot search some data - the search will be performed on all rows.



&#x20;   If property name ends with ...SearchS the search index will only be&#x20;
using spaces to get words (as opposed to spaces and special characters).
&#x20;For example, string "hi\@test me" would normaly satisfy search queries&#x20;
made of words: hi, test, me, hi\@test.
&#x20;   If, however, the property being searched ends with ...SearchS it&#x20;
will only satisfy queries containing these words: hi\@test, me.



&#x20;   All operations on strings are case-insensitive. Under the hood&#x20;
.Search is a simple on-disk inverted index, partitioned per batches of&#x20;
documents.


**Bitfunnel search**

Bitfunnel is efficient search algorithm that uses bloom filters. To create in-memory bitfunnel index on a string column:

```
var ratio = db.Table<SomeData>()
            .CreatePropertyBitFunnelMemoryIndex(f => f.StringVal, 160, 2048);

```


&#x20;   Second parameter is a bloom filters size in bits. The longer the&#x20;
text of StringVal property in general, the greater length is needed.&#x20;
Returned ratio indicates the set bits to all bits ratio in a bloom&#x20;
filter given that index
&#x20;   is being created on some existing data. This parameter decides how&#x20;
many false-positives the search will return. The next parameter is the&#x20;
documents batch size the bloom filter is created on.
&#x20;   Default is 1024 documents. The smaller the batch the easier it is to
&#x20;perform modifications operations, but the larger the batch - the faster
&#x20;and more memory efficient is the search.
&#x20;   After index is created it will be maintained automatically and&#x20;
rebuilt on server reboot just like other in-memory indexes until it is&#x20;
removed by RemoveBitFunnelMemoryIndex. In order to make actual search:


```
var res = db.Table<SomeData>()
            .SearchBitFunnel(f => f.StringVal, "some query")
            .GetIds();

```


&#x20;   Bitfunnel will return true results as well as false positives. Under
&#x20;the hood 3 hash functions are used. Note for maximum efficiency GetIds&#x20;
is used in the example.


**Vector search**

Vector search is supported that doesn't require a lot of RAM.&#x20;
Property must have float[] type and end with L2 - then an on-disk index&#x20;
will be built on it automatically.
Such property can be searched using SearchNeighbours method:

```

    var res = db.Table<SomeData>()
    .SearchNeighbours(f => f.MyVectorL2, target_vector, k).SelectEntity();

```


&#x20;   LinqdbSelectStatistics can be supplied to .Select or .SelectEntity&#x20;
to obtain dictionary of (Id, Distance) of returned vectors.
&#x20;   Works on partitioned tables as well.


**Or**

By default when statements like `.Where` or `.Search` go together they imply logical "and" to the results. It is possible to have "or" on neighbouring statements like so:

```
var res = db.Table<SomeData>()
            .Search(f => f.Name, "some text").Or().Search(f => f.Name, "something else")
            .Where(f => f.Id > 100)
            .Select(f => new
            {
                Id = f.Id,
                Name = f.Name
            });

```

In this case only one of search need to satisfy to return the result. More than one `.Or` could be used.

**Intermediate results**

All the work happens in `.Select` or `.SelectEntity` statement (generally - in the last statement), so things like these are possible:

```
var tmp = db.Table<SomeData>()
             .Where(f => f.Normalized == 5);
if (person_id != null)
{
    tmp.Where(f => f.PersonId == person_id); //no need to assign to tmp
}
var res = tmp.SelectEntity();

```

**Count**

To obtain table count:

```
db.Table<SomeData>().Count();

```

Also works when conditions applied:

```
db.Table<SomeData>().Where(f => f.Id < 50).Count();

```

**GetIds**


&#x20;   GetIds() method allows to get list of id's satisfying conditions&#x20;
without having to select them. Id (or \<Sid, Id> in case of&#x20;
.DistributedTable) is a special column - it's value is encoded in every&#x20;
column and carried around in all operations.
&#x20;   That's why GetIds have it without having to do select as with other&#x20;
columns.


### Updating

`Save(item);` will update row with item’s `Id` using item’s properties, given that `Id` is not 0. If it is 0, new item will be created and new `Id` will be assigned to the object’s `Id` property.

To update column of multiple rows you would need to construct `Dictionary<int, T>` where `T` is column’s type and:

```
var dic = new Dictionary<int, int?>();
dic[2] = 8;
dic[3] = 11;
db.Table<SomeData>().Update(f => f.PeriodId, dic);

```

This will update column `PeriodId` of rows with Ids 2 and 3 with respective values 8 and 11.

### Deleting

To delete row(s) you would need to construct `HashSet<int>` of ids to be deleted and:

```
db.Table<SomeData>().Delete(new HashSet<int>() { 2, 3 });

```

### Atomic increment

To increment values atomically. i.e. thread-safe, use `.AtomicIncrement`:

```
var new_item = new Counter() { Name = "unique_name", Value = 1 };
db.Table<Counter>().Where(f => f.Name == "unique_name").AtomicIncrement(f => f.Value, 1, new_item, null);

```

### Transactions

Linqdb supports transactions:

```
using (var transaction = new LinqdbTransaction())
{
    var d = new SomeData()
    {
        Id = 1,
        Normalized = 1.2,
        PeriodId = 5
    };
    db.Table<SomeData>(transaction).Save(d); //note that .Table takes transaction as a parameter
    var d2 = new BinaryData()
    {
        Id = 1,
        Data = new List<byte>() { 1, 2, 3 }.ToArray()
    };
    db.Table<BinaryData>(transaction).Save(d2);
    transaction.Commit(); //all writes happen here, if it fails - nothing gets modified (all or nothing)
}

```


&#x20;   if `.Commit` is not called, nothing is modified. Transaction must be created used and destroyed in the same thread.
&#x20;   After `.Save` (or `.SaveBatch`) inside a transaction new items (with `Id` == 0) are assigned new ids, so that they can be used before doing commit.
&#x20;   Transaction is supported on these data-modifying commands: `.Save`, `.SaveBatch`, `.Update`, `.Delete`.


### Indexes


&#x20;   Linqdb supports in-memory indexes on columns of type `int`, `DateTime`, `double`, `long` and `decimal`. In-memory indexes speed up reading operations on such columns.
&#x20;   Also indexes are required for `.GroupBy` statement (see below). To build an index on property use `.CreatePropertyMemoryIndex`, to remove - `.RemovePropertyMemoryIndex`
&#x20;   Indexes slow down data-modifying operations, so use them only when&#x20;
you have to. Before using indexes consider using other features for&#x20;
improving performance: DistributedTable or PartitionedTable, or&#x20;
DistributedPartitionedTable.
&#x20;   If indexes (or searchable string properties) are present it is&#x20;
especially efficient to use batch operations, i.e. .SaveBatch and to&#x20;
make batches larger.


### GroupBy

Suppose you want to group by column `PeriodId` and aggregate column `Value`. For that in-memory index must be created:

```
db.Table<SomeData>().CreateGroupByMemoryIndex(f => f.PeriodId, f => f.Value);

```

Then you could do something like:

```
var res = db.Table<SomeData>()
            .GroupBy(f => f.PeriodId)
            .Select(f => new { f.Key, Sum = f.Sum(z => z.Value), Avg = f.Average(z => z.Value) });

```


&#x20;   These aggregation functions are supported: `Count`, `CountDistinct`, `Sum`, `Max`, `Min`, `Average`. `Key` selects group-by property's value.


### Partitioned table


&#x20;   Partitioned table is for the following situation: when you have a&#x20;
large table but perform operations only on some slices of it. For&#x20;
example, you have stock prices but only work with per company data.
&#x20;   In such case partitioned tables offer great performance boost.&#x20;
Example of usage:



&#x20;   ` 
        var result = db.PartitionedTable<PricePoint>("msft").SelectEntity();
     `



&#x20;   PartitionedTable takes partition argument which in this case is&#x20;
symbol of a company. Under the hood partitioned table is actually&#x20;
separate table and as such records in Rocksdb start with same unique&#x20;
prefix.
&#x20;   Since those records are sorted they will be in same file or much&#x20;
smaller number of files compared to the case if table wouldn't be&#x20;
partitioned. This fact makes operations on them fast.
&#x20;   The downside of partitioned table is you have to make multiple calls
&#x20;to the db if you're working on several partitions. Perhaps for those&#x20;
operations data could be duplicated on a non-partitioned table.

&#x20;   Transactions are not supported on partitioned tables.


### Distributed db (aka sharding)


&#x20;   The idea is to have a table that is simillar to regular .Table but&#x20;
under the hood the data is distributed among N servers. This way you can
&#x20;scale your database using similar code.
&#x20;   To create distributed database you just create N servers, construct&#x20;
usual Db objects and pass them to DistributedDb:


```

        var list = new List<Db>()
        {
        new Db("ip:port", "user", "pass"),
        new Db("ip:port", "user", "pass"),
        new Db("ip:port", "user", "pass"),
        new Db("ip:port", "user", "pass"),
        new Db("ip:port", "user", "pass"),
        new Db("ip:port", "user", "pass"),
        };
        var distributeddb = new DistributedDb(list.Select((f, i) => new { Db = f, index = i }).ToDictionary(f => f.index + 1, f => f.Db));
    
```


&#x20;   Now distributeddb has .DistributedTable which has operations simillar to .Table. Here are the differences:
&#x20;   

-
  &#x20;               Entity class now must have Id and Sid (server id)&#x20;
  properties. This pair identifies a record and is called DistributedId.&#x20;
  They are only going to be used in data modification
  &#x20;               operations: Delete and Update. To uniquely identify a&#x20;
  record you would give it a Guid property without relying on Id and Sid.
  &#x20;           
-
  &#x20;               No transactions are supported on distributed table. It&#x20;
  is possible that data might be saved on one server and throw exception&#x20;
  (not saved) on the other.
  &#x20;           
-
  &#x20;               GroupBy statement returns `dynamic` result. Less aggregation functions are supported: `Sum, Count, Min, Max, Average `
  &#x20;           


&#x20;   Some other notes:


- data is distributed randomly and uniformly among servers.
- if one server is down - the whole operation breaks.
- DistributedPartitionedTable is also supported
- IntersectWithDistributedIds is available for distributed tables

### Generic types


&#x20;   One of a coolest features of Linqdb: it supports working on generic types: `db.Table<T>().`. So yo can have a generic save and get methods:


```

public static void SaveGeneric<T>(T item) where T: new()
{
    var db = DbFactory.GetDb();
    var res = db.Table<T>().Save(item);
}

public static List<T> GetGeneric() where T: new()
{
    var db = DbFactory.GetDb();
    var res = db.Table().SelectEntity();
    return res;
}

```

This way you can save and retrieve different entities just by using&#x20;
these two methods. You could also define interfaces with some common&#x20;
properties and work with these properties in these generic methods too.

### Non-atomic modifications and select


&#x20;   There is a certain limit on batch sizes in modification operations,&#x20;
so that they could be performed atomically (all or nothing) using ACID&#x20;
principles. If there is a need to modify large amount of data and ACID&#x20;
principles are not
&#x20;   important, there are convenience methods: `SaveNonAtomically`, `UpdateNonAtomically`, `DeleteNonAtomically`. These cannot be used in a transaction. 

&#x20;   Same goes to Select vs SelectNonAtomically. Select does reading&#x20;
using ACID principles with a specific data-snapshot. However, there is a
&#x20;limit of how much data can it read in one go. If ACID is not important
&#x20;   (like, for example, when no one is modifying data)&#x20;
SelectNonAtomically can read whatever amount of data in iterations using
&#x20;Select of smaller batches. So non-atomic versions are just convinience&#x20;
methods of iteratively calling their ACID-compliant counterparts.


### Server status


&#x20;   GetServerStatus(int timeToWaitInMs = 1000) returns server's name&#x20;
(servername property in config.txt) and also returns no later than&#x20;
timeToWaitInMs ms. If server doesn't respond sooner it is considered&#x20;
down.


### Queues


&#x20;   Simple, but fast in-memory queues are supported: `db.Queue<T>().PutToQueue` adds item to queue T and `db.Queue<T>().GetAllFromQueue`
&#x20;gets all queue's T items and removes them from queue.
&#x20;   Only one reader is quaranteed to get specific item in multi-threaded
&#x20;environment. Entities used in a queue must be marked with protobuf&#x20;
attributes.


**Named queues**


&#x20;   Also named queues are supported with following operations: `db.NamedQueue<T>("some name").PutReplaceInNamedQueue(List<T> items)` - replaces all items in named queue with given items;
&#x20;   `db.NamedQueue<T>("some name").GetAllFromNamedQueue`
&#x20;\- returns all items from the queue and optionally removes them from the
&#x20;queue. Again atomicity and thread-safety of operations is provided.


### Init \ dispose

Before using Linqdb one would need to create it:

`      var db = new Db("DATA"); //embedded version     var db = new Db("host_ip:port"); //server's client version  `  


&#x20;   This db object should be a singleton as shown in DbFactory example above.
&#x20;   The argument is path to the database or ip to the server. 

&#x20;   Before exiting application it is a good practice to dispose the db (although currently this does nothing):


```
db.Dispose();

```

During the lifetime of application db object shouldn't be disposed.

### Replication

To make a copy of a database programatically:

```
db.Replicate("PATH_TO_COPY");

```

If directory exists - it will be removed before copying. Database can still be read/written while replication is in progress.

Not tested on large database under heavy load.

### Limitations \ caveats

-
  &#x20;               Embedded Linqdb is one process database, that is only&#x20;
  one process can access it at a time, even if it’s just for reading.
  &#x20;           
-
  &#x20;               You cannot change type of a column. If you need that –&#x20;
  create a new column with required type and copy data there. (Changing&#x20;
  type from nullable to non-nullable and vice versa is ok.)
  &#x20;           
-
  &#x20;               And after you modify type after it has been created -&#x20;
  say added new column - you may want to manually fill it with whatever&#x20;
  default value (i.e. if it is a nullable type – it won’t automatically&#x20;
  satisfy == null after creation).
  &#x20;               In other words, type changes don’t automatically change&#x20;
  data.
  &#x20;           
-
  &#x20;               You can't select data from two or more tables in one&#x20;
  data snapshot.
  &#x20;               More so, if you use SelectNonAtomically there is a risk&#x20;
  that someone will modify data while select is executing and it will&#x20;
  return a row that doesn't match the Where statement.
  &#x20;               If you need ACID operations, then you can achieve those&#x20;
  within one table if you don't use methods that have NonAtomically in&#x20;
  their names.
  &#x20;           
- Try to keep Ids of table records sequential and starting from 1. This way search and other operations are most efficient.
- There is currently no fail-safe mechanism. If server is&#x20;
  down, database is down. If one of the servers of distributed db is down,
  &#x20;the whole distributed db is down.

### &#x20;    Server installation 


&#x20;       On Windows server folder contains `Server.exe` which starts listening on port specified in `config.txt`

&#x20;       Server has configuration file `config.txt` which is self-explanatory.

&#x20;       You could install server as Windows service using [NSSM](http://nssm.cc/).