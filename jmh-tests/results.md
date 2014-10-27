Intermediate result of LRUListBenchmark_Complex:
```
Benchmark                                (cacheType)   Mode   Samples        Score  Score error    Units
Complex.thread1                         
Complex.thread1:testAdd_1thread           concurrent  thrpt        10     3344.092     1293.573   ops/ms
Complex.thread1:testGet_1thread           concurrent  thrpt        10    22151.559     8408.290   ops/ms
Complex.thread1:testRemoveLRU_1thread     concurrent  thrpt        10     9356.151    28304.728   ops/ms

Complex.thread1                         
Complex.thread1:testAdd_1thread         synchronized  thrpt        10     1708.707      751.146   ops/ms
Complex.thread1:testGet_1thread         synchronized  thrpt        10    16810.899     2152.510   ops/ms
Complex.thread1:testRemoveLRU_1thread   synchronized  thrpt        10     1839.474     1286.904   ops/ms

Complex.thread2                           
Complex.thread2:testAdd_2thread           concurrent  thrpt        10     3579.881     1566.606   ops/ms
Complex.thread2:testGet_2thread           concurrent  thrpt        10    15588.109     7133.335   ops/ms
Complex.thread2:testRemoveLRU_2thread     concurrent  thrpt        10    12252.225     9146.958   ops/ms

Complex.thread2                       
Complex.thread2:testAdd_2thread         synchronized  thrpt        10     2792.537      792.950   ops/ms
Complex.thread2:testGet_2thread         synchronized  thrpt        10     5559.479      389.331   ops/ms
Complex.thread2:testRemoveLRU_2thread   synchronized  thrpt        10     3683.578     2332.648   ops/ms

Complex.thread4                          
Complex.thread4:testAdd_4thread           concurrent  thrpt        10     2741.485     1594.393   ops/ms
Complex.thread4:testGet_4thread           concurrent  thrpt        10    11798.647     7287.306   ops/ms
Complex.thread4:testRemoveLRU_4thread     concurrent  thrpt        10    11730.609     8459.239   ops/ms

Complex.thread4                         
Complex.thread4:testAdd_4thread         synchronized  thrpt        10     2068.359      265.544   ops/ms
Complex.thread4:testGet_4thread         synchronized  thrpt        10    12447.852     1248.437   ops/ms
Complex.thread4:testRemoveLRU_4thread   synchronized  thrpt        10     2981.975     2215.068   ops/ms
```