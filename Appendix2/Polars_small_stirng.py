import polars as pl
import time
import tracemalloc

df = pl.read_csv('C:/Users/manvi/OneDrive/Documents/Thesis Stuffs/Thesis Report/Experimentation/small dataset/imdb_movies.csv', ignore_errors=True)

# String Concatenation
print("String Concatenation")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

concatenation_df = df.select(pl.col('names') + pl.col('genre'))

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Convert to Upper Case
print("Convert to Upper Case")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

upper_case_df = df.select(pl.col('names').str.to_uppercase())

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Convert to Lower Case
print("Convert to Lower Case")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

lower_case_df = df.select(pl.col('names').str.to_lowercase())

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Length of String
print("Length of String")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

length_df = df.select(pl.col('names').str.len_bytes())

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# String Replace
print("String Replace")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

replace_df = df.select(pl.col('genre').str.replace("Family", "Drama"))

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# String Split
print("String Split")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

split_df = df.select(pl.col('names').str.split("_"))

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Extract Substring
print("Extract Substring")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

substring_df = df.select(pl.col('genre').str.slice(1, 2))

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Regular Expression Match
print("Regular Expression Match")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

regex_match_df = df.select(pl.col('genre').str.contains("Drama"))

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))


# Check String Start and End
print("Check for String Start and End")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

starts_with_df = df.select(pl.col('genre').str.starts_with('Drama'))
ends_with_df = df.select(pl.col('genre').str.ends_with('Action'))

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))


# Extract with Regular Expression
print("Extract with Regular Expression")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

extract_regex_df = df.select(pl.col('genre').str.extract(r'(,)', 0))

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Extract all with regular expression
print("Extract All with Regular Expression")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

extract_all_regex_df = df.select(pl.col('genre').str.extract_all(r'(,)'))

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Match regular expression
print("Match Regular Expression")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

match_regex_df = df.select(pl.col('genre').str.contains(r'^Drama$'))

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# Remove Whitespaces
print("Remove Whitespaces")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

strip_df = df.select(pl.col('overview').str.strip_chars())

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))

# String Zfill
print("String Zfill")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

zfill_df = df.select(pl.col('overview').str.zfill(10))

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))
