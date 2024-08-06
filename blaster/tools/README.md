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
