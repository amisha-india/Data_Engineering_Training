import numpy as np

#Creating a one-dimensional array
arr = np.arry([1,2,3,4,5])
print("Array:",arr)

# Reshape to a 2*3 array
reshape_arr = np.arrange(6).reshape(2,3)
print("Reshaped Array:\n", reshape_arr)

# Element - wise addition
arr_add = arr+10
print("Added 10 to each element:", arr_add)

#Element - wise multiplication
arr_mul = arr*2
print("multiplied each element by 2:", arr_mul)

# slicing arrays
sliced_arr = arr[1:4]# get element from index 1 to3
print("sliced Array:", sliced_arr)