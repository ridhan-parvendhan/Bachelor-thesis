import pandas as pd
import time 
import tracemalloc
# Converting dataset into a pandas DataFrame
df = pd.read_csv('C:/Users/manvi/OneDrive/Documents/Thesis Stuffs/Thesis Report/Experimentation/Large Dataset/Google-Playstore.csv')

# Sum Aggregation
print("Sum Aggregation")
# To calculate time and memory before executing the operation
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()
# Operation
sum_df = df['Maximum Installs'].sum()
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

variance_df = df['Rating Count'].var()

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

std_dev_df = df['Maximum Installs'].std()

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

sorted_df = df.sort_values(by='Rating')

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

filtered_df = df[df['Rating Count'] > 5000]

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

no_null_df = df["Rating Count"].dropna()

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

filled_df = df["Minimum Installs"].fillna(0)

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

corr_df = df['Rating'].corr(df['Rating Count'])

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

cov_df = df['Minimum Installs'].cov(df['Maximum Installs'])

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

desc_df = df['Rating'].describe()

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

groupby_df = df.groupby('Ad Supported').agg({'Rating': 'mean'})

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

pivot_df = df.pivot_table(index='Ad Supported', columns='Price', values='Rating', aggfunc='mean')

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

normalized_df = (df['Rating'] - df['Rating'].min()) / (df['Rating'].max() - df['Rating'].min())

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

