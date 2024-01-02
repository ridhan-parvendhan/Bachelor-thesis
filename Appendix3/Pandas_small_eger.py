import pandas as pd
import time
import tracemalloc

#Converting Dataset into Pandas DataFrame
df = pd.read_csv('C:/Users/manvi/OneDrive/Documents/Thesis Stuffs/Thesis Report/Experimentation/small dataset/imdb_movies.csv')

# Filtering
print("Filtering")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

filtered_df_pandas = df[df['score'] > 60]

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Group BY Aggregation
print("Group By Aggregation")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

grouped_df_pandas = df.groupby('score').agg({'revenue': 'sum'})

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

sorted_df_pandas = df.sort_values(by='score')

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

selected_columns_df_pandas = df[['score', 'revenue']]

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

complex_filtered_df_pandas = df[(df['score'] > 60) & (df['revenue'] < 70000000)]

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

renamed_df_pandas = df.rename(columns={'score': 'rating'})

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

df_dropped_columns_pandas = df.drop(columns=['crew', 'country'])

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

regex_matched_df_pandas = df[df['genre'].str.contains(r'.*action$', na=False)]

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))
