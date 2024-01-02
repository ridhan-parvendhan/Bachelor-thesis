import pandas as pd
import time
import tracemalloc

df = pd.read_csv('C:/Users/manvi/OneDrive/Documents/Thesis Stuffs/Thesis Report/Experimentation/Large Dataset/Google-Playstore.csv')

# String Concatenation
print("String Concatenation")
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()
start_time = time.time()

concatenation_df = df['App Name'] + df['Category']

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

upper_case_df = df['App Name'].str.upper()

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

lower_case_df = df['App Name'].str.lower()

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

length_df = df['App Name'].str.len()

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

replace_df = df['Category'].str.replace('House', 'Home')

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

split_df = df['App Name'].str.split('_')

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

substring_df = df['Developer Email'].str[1:3]

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

regex_match_df = df['Developer Email'].str.contains('@gmail.com')

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

starts_with_df = df['Developer Website'].str.startswith('http')
ends_with_df = df['Developer Website'].str.endswith('.com')

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

extract_regex_df = df['Developer Email'].str.extract(r'(@gmail.com)')

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

extract_all_regex_df = df['Developer Website'].str.findall(r'(.com)')

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

match_regex_df = df['Category'].str.match(r'(Game)')

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

strip_df = df['Developer Email'].str.strip()

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

zfill_df = df['Developer Website'].str.zfill(width=10)

end_time = time.time()
snapshot2 = tracemalloc.take_snapshot()
tracemalloc.stop()
execution_time= end_time - start_time
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_used = sum(stat.size_diff for stat in top_stats)
print("Execution time: {} seconds".format(execution_time))
print("Memory used: {} bytes".format(memory_used))