a = 10
b = 3
# Arithmetic operation

# Addition
print(a+b)
# subtraction
print(a-b)
# Multiplication
print(a*b)
# Division
print(a/b) # this return a float
# Modulus
print(a%b) # this give remainder
# Exponentiation
print(a**b)

# type casting

x= 10
y = 5
# comparison operators
is_greater = x > y # give bollean value
is_equal = x==y
print(is_greater)
print(is_equal)

#################
a = True
b = False
# logical operator
and_op = a and b # a * b = 0 * 1= 0
or_op = a or b # true + false = true = 1
not_op = not a # false

print(and_op)
print(or_op)
print(not_op)

# convert intger to boolean
bool_from_int = bool(1) # True

#convert zero to boolean
bool_from_zero = bool(0) # false

# convert string to boolean
bool_from_str = bool("Hello") # true

# convert empty string to boolean
bool_from_empty_str = bool("")# false

print("boolean from integer 1 :", bool_from_int)
print("boolean from integer 0 :", bool_from_zero)
print("boolean from non-empty string: ",bool_from_str)
print("boolean from empty string :", bool_from_empty_str)