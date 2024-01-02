import polars as pl
import time
import tracemalloc

#Converting Dataset into Polars DataFrame
df = pl.read_csv('C:/Users/manvi/OneDrive/Documents/Thesis Stuffs/Thesis Report/Experimentation/Large Dataset/Google-Playstore.csv', ignore_errors=True)

# Filtering
print("Filtering")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

filtered_df_polars_lazy = df.lazy().filter(pl.col('Rating') > 3)

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Group By Aggregation
print("Group By Aggregation")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

grouped_df_polars_lazy = df.lazy().group_by('Rating').agg(pl.col('Rating Count').sum())

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Sorting
print("Sorting")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

sorted_df_polars_lazy = df.lazy().sort('Rating')

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Select Columns
print("Select Columns")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

selected_columns_df_polars_lazy = df.lazy().select(['Rating', 'Rating Count'])

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Complex Filtering
print("Complex Filtering")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

complex_filtered_df_polars_lazy = df.lazy().filter((pl.col('Rating') > 3) & (pl.col('Rating Count') < 500))

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Column Renaming
print("Column Renaming")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

renamed_df_polars_lazy = df.lazy().rename({'Category': 'Type'})

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Drop Columns
print("Drop Columns")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

df_dropped_columns_polars_lazy = df.lazy().drop(['Minimum Installs', 'Maximum Installs'])

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# String Matching
print("String Matching")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

regex_matched_df_polars_lazy = df.lazy().filter(pl.col('Developer Email').str.contains(r'.*@gmail\.com$'))

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))
