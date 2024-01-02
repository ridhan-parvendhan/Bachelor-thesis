import polars as pl
import time
import tracemalloc
# Converting dataset into a Polars DataFrame
df = pl.read_csv('C:/Users/manvi/OneDrive/Documents/Thesis Stuffs/Thesis Report/Experimentation/small dataset/imdb_movies.csv')

# Sum Aggregation
print("Sum Aggregation")
# To calculate time and memory before executing the operation
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()
# Operation
sum_df = df.select(pl.sum('budget_x'))
# To calculate time and memory after executing the operation
end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Variance Calculation
print("Variance Calculation")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

variance_df = df.select(pl.var('revenue'))

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Standard Deviation 
print("Standard Deviation")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

std_dev_df = df.select(pl.std('budget_x'))

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

sorted_df = df.sort('score')

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Filtering
print("Filtering")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

filtered_df = df.filter(pl.col('revenue') > 5000)

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Null Value Removing
print("Null Value Removing")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

no_null_df = df.select(pl.col('score').drop_nulls())

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Null Value Replacing
print("Null Value Replacing")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

filled_df = df.with_columns(pl.col('budget_x').fill_nan(0))

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Correlation Calculation
print("Correlation Calculation")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

corr_df = df.select(pl.corr('score', 'revenue'))

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Covariance Calculation
print("Covarince Calculation")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

cov_df = df.select(pl.cov('budget_x', 'revenue'))

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Descriptive Statistics
print("Descriptive Statistics")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

desc_df = df.select(pl.col('score')).describe()

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

groupby_df = df.group_by('genre').agg(pl.col('score').mean())

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Pivot Table
print("Pivot Table")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

pivot_df = df.pivot(index='genre', columns='country', values='score', aggregate_function='mean')

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Data Normalization
print("Data Normalization")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

normalized_df = (df.select(pl.col('score')) - df.select(pl.min('score'))) / (df.select(pl.max('score')) - df.select(pl.min('score')))

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))
