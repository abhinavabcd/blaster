#  Documentation of Functions in tools (`__init__.py`)

# List_diff2 Function

## Description
`list_diff2` is a function that computes the difference  between two lists.
## Arguments
- **first**: *list*
  - The first list to compare.
- **second**: *list*
  - The second list to compare.
- **key**: *function*, optional
  - A function that extracts a comparison key from each list element. If not provided, the elements themselves are compared.

## Returns
- tuple: A tuple containing two lists:
    - The first list contains elements that are in **first** but not in **second**.
    - The second list contains elements that are in **second** but not in  **first**
	
## Usage
``` python 
first = [1,2,3]
second = [2,3,4]
reult = list_diff2(first, second)
# Output: ([1], [4])
```

# Convert a datetime  object to a timestamp.

## **Description**
The date2timestamp function converts a datetime  object to a timestamp, .

## **Arguments**
- ** dt **: The value to be converted. 

## **Returns**
- ** float or original type **: 
  - If **dt** is a `datetime` object, the function returns a timestamp as a float.
  - If **dt** is not a `datetime` object, it returns `dt` unchanged.

## **Usage**
```python
dt = datetime(2023, 5, 15, 12, 30, 0)
timestamp = date2timestamp(dt)
print(timestamp)  # Output is  1684153800.0
 
```



## Convert Duration in Seconds to Human-Readable String

## **Description**
The **duration2string** function takes a duration given in seconds and converts it into a human-readable string format. The output can display in day-hours-minutes-seconds 

## **Arguments**
- **seconds** (int): The total duration in seconds that needs to be converted.

## **Returns**
- `str`: A string representing the duration in the format of days (`d`), hours (`h`), minutes (`m`), and seconds (`s`).


## **Usage**
```python
duration1 = 200023  
result1 = duration2string(duration1)
print(result1)  # Output: "2d7h33m43s"
```
##### Conversion 
* Days: 90061 // 86400 = 2 days
* Remaining seconds: 90061 - (2 * 86400) = 27223
* Hours: 27223 // 3600 = 7 hours
* Remaining seconds: 27223 - (7* 3600) = 2023
* Minutes: 2023 // 60 = 33 minutes
* Remaining seconds: 2023 - (33 * 60) = 43
* Seconds: 43



## Convert string to the duration in seconds 
**Description:**
The **string2duration **  function converts a duration string into the total number of seconds. The duration string can include days, hours, minutes, and seconds and calculates the duration in seconds.  

**Arguments:**
- `duration_str` *(str)*: A string representing a duration. The string should be in format of day(`d`)-hours(`h`) -minutes(`m`)-seconds(`s`).
Example: `**2d5h30m15s**.

**Returns:**
- *(int)*: The total duration represented by the input string, expressed in seconds.

**Usage:**

```python
duration = "2d5h30m15s"
total_seconds = string2duration(duration)
print(total_seconds)  # Output will be 194415 seconds

```
##### Conversion 
* Days: 2d → 2 * 86400 = 172800 seconds
* Hours:5h → 5 * 3600 = 18000 seconds
* Minutes:30m → 30 * 60 = 1800 seconds
* Seconds:15s → 15 seconds
* Total_seconds  = 194415 seconds 



# Removing duplicates from the list

## Description

The **remove_duplicates** function takes a list and removes duplicate elements, returning a new list with only unique elements. 

##Arguments

- **lst** (list): The input list from which duplicates will be removed.
- **key**(function, optional): A function that extracts a comparison key from each element. If provided, duplicates are determined based on the values returned by this function.

## Returns

- (list): A new list containing unique elements from the input list, preserving the order of their first occurrence.

## Usage

```python
# Removing duplicates from a list of integers
numbers = [1, 2, 2, 3, 4, 4, 5]
unique_numbers = remove_duplicates(numbers)
print(unique_numbers)  # Output: [1, 2, 3, 4, 5]

#  Removing duplicates from a list of strings
words = ["apple", "banana", "apple", "orange", "banana"]
unique_words = remove_duplicates(words)
print(unique_words)  # Output: ["apple", "banana", "orange"]

# Example 3: Removing duplicates using a key function
objects = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}, {'id': 1, 'name': 'Alice'}]
unique_objects = remove_duplicates(objects, key=lambda x: x['id'])
print(unique_objects)  # Output: [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
```



# Read Rows from a URL

## **Description**:
This function reads rows of data from a file located at a specified URL. The function supports reading data from CSV, XLS, and XLSX files. It returns an iterator that yields each row of the file as a list. 

For CSV files, the rows are split based on a specified delimiter. For Excel files, the function reads all the rows in the first sheet.

## **Arguments**:
- **url (str)**: 
  The URL pointing to the file that needs to be read. The URL can end in `.csv`, `.xls`, or `.xlsx` to specify the file type.
  
- **csv_delimiter (str)**:
  An optional parameter that specifies the delimiter used in the CSV file. The default value is a comma (`,`).

## **Returns**:
- **generator**:
  An iterator that yields each row as a list of values. The type of each value depends on the file format:
  - For `.csv` files, each row is a list of strings.
  - For `.xlsx` files, each row is a tuple with the cell values.
  - For `.xls` files, each row is a list of strings.

##**Usage**:
```python
url = "https://example.com/data.csv"
for row in read_rows_from_url(url):
    print(row)
```
| Month | 1958 | 1959 | 1960 |
|-------|-------|-------|-------|
| JAN   | 340   | 360   | 417   |
| FEB   | 318   | 342   | 391   |
| MAR   | 362   | 406   | 419   |
| APR   | 348   | 396   | 461   |
| MAY   | 363   | 420   | 472   |
| JUN   | 435   | 472   | 535   |
| JUL   | 491   | 548   | 622   |
| AUG   | 505   | 559   | 606   |
| SEP   | 404   | 463   | 508   |
| OCT   | 359   | 407   | 461   |
| NOV   | 310   | 362   | 390   |
| DEC   | 337   | 405   | 432   |



# ThreadPool Class

# Description
A class that manages a pool of threads, which consume tasks from a single queue. This allows for concurrent execution of tasks using a fixed number of threads.

## Attributes
- **tasks (Queue)**: A queue that holds the tasks to be executed by the worker threads.
- **worker_threads (list)**: A list that stores the worker threads in the thread pool.

##Methods

## __init__(self, num_threads)
Initializes the thread pool with a specified number of threads.

**Args:**
- **num_threads (int)**: The number of threads to create in the pool.

**Details:**
- Initializes the `tasks` queue with a maximum size equal to `num_threads`.
- Creates and starts the worker threads, which immediately begin consuming tasks from the queue.

## add_task(self, func, *args, **kargs)
Adds a task to the queue to be processed by the worker threads.

**Args:**
- **func (callable)**: The function that represents the task to be executed.
- ** *args**: Positional arguments to pass to the function.
- ****kargs**: Keyword arguments to pass to the function.

**Details:**
- The function and its arguments are packaged as a tuple and put into the task queue.

## join(self)
Waits for the completion of all tasks in the queue and then stops the worker threads.

**Details:**
- Blocks until all tasks in the queue have been processed.
- Signals all worker threads to stop running by setting their `is_running` attribute to `False`.
- Joins all worker threads, ensuring that they have terminated before continuing.


## Parse Command Line Arguments

## **Description**:
The `parse_cmd_line_arguments` function processes command-line arguments passed to a script. It distinguishes between positional arguments and options with or without values. Positional arguments are collected in a list, while options and their values are stored in a dictionary. 
#### **Arguments**:
- **No arguments**: This function uses `sys.argv` to access command-line arguments, so no parameters are needed when calling it.

#### **Returns**:
- **args (list)**:
  A list of positional arguments provided on the command line. These are arguments that are not associated with any options.

- **args_map (dict)**:
  A dictionary where keys are option names and values are their corresponding values. Options are parsed from command-line arguments that start with a hyphen (`-`). If an option has an equals sign (`=`), it is parsed as a key-value pair; otherwise, the following argument is considered the value for the option.

#### **Usage**:
To use the **parse_cmd_line_arguments** function, run the script from the command line with appropriate arguments. The function will parse these arguments and return them in a structured format.


##Retry Decorator for Handling Function Failures

##Description:
The **retry`**function is a decorator that wraps another function, enabling it to retry execution in case of specific exceptions. It retries the function a specified number of times (`num_retries') with a delay between attempts, while optionally ignoring certain exceptions. This decorator is useful for functions that may fail intermittently due to transient issues, such as network requests.

## **Arguments**:
- **num_retries (int)**:
  The number of times to retry the function if it fails. Default is `2`. The value is automatically set to a minimum of `2` to ensure at least one retry occurs.

- **ignore_exceptions (list)**:
  A list of exception types that should be caught and ignored during retries. If an exception is in this list, the function will not raise it immediately but will instead retry. Default is `None`, meaning no exceptions are ignored unless specified.

- **max_time (int)**  The maximum amount of time in milliseconds allowed for all retries combined. The sleep time between retries is calculated as `max_time / num_retries`. Default is `5000` milliseconds (5 seconds).

## **Returns**:
- The `retry` function returns a decorator that wraps the target function with retry logic.

## **Usage**:
To use the `retry` decorator, apply it to any function you want to make more resilient to transient errors. The function will automatically retry on failure, according to the specified parameters.




# Submit a Background Task for Execution

## **Description**:
The **submit_background_tas**` The function submits tasks for asynchronous processing by managing task partitioning and ensuring that background threads are started if they are not already running. It queues tasks for execution and supports partitioning across multiple parallel buckets, allowing for efficient execution of tasks in the background.

## Arguments:
- **partition_key (str or int)**:
  An optional key used to determine which partition (or "bucket") the task should be assigned to. If `partition_key` is `None`, it defaults to the current time in milliseconds modulo 50, spreading tasks across 50 partitions.
  
- **func (function)**:
  The function that will be executed as the background task.

- ***args**:
  Positional arguments to be passed to the `func` when it is executed.

- ****kwargs**:
  Keyword arguments to be passed to the `func` when it is executed.

## **Returns**:
- **None**:
  This function does not return a value. It submits the task to a queue for background execution.

## Usage:
```python
def example_task(x, y):
    print(f"Running example_task with x={x} and y={y}")
    return x + y

# Submit the task with a specific partition key
submit_background_task(partition_key="example_key", func=example_task, x=5, y=10) #15

# Submit the task without specifying a partition key
submit_background_task(partition_key=None, func=example_task, x=7, y=3) #10
```



#Condense Words for Efficient Search

## **Description**:
The **condense_for_search** function processes a variable number of input strings or lists of words, condensing them by mapping words to their first five characters (or a shorter prefix if the word is less than five characters). It creates a dictionary to store these mappings and then returns a list of words representing the most relevant words for search purposes. This function can be useful in scenarios where quick lookups or indexing of words based on their prefixes are needed.

##**Arguments**:
- **args**:
  A variable number of arguments that can be either strings or lists of words. Each argument is processed, and words are condensed based on their first five characters.

## **Returns**:
- **ret (list)**:
  A list of words condensed based on their prefixes. This list contains the most relevant words from the input arguments, with longer lists of words replacing shorter ones if they share the same prefix.

##**Usage**:
To use the `condense_for_search` function, simply pass strings or lists of words as arguments. The function will return a condensed list of words based on their first five characters.

```python
words1 = "apple banana orange grape"
words2 = "application banner organize grapefruit"
result = condense_for_search(words1, words2)
print(result) # Output: ['apple', 'banana', 'orange', 'grapefruit', 'application', 'banner', 'organize']
```

