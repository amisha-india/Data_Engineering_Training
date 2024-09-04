# list (array of  any data type)
# creating list
empty_list = []
num = [1,2,3,4,5]
mixed_list = [1, "hello", 3.13,True]
print(num)
print(empty_list)
print(mixed_list)
# accessing list element
first_element = num[0]
last_element = num[-1]
print(last_element)
print(first_element)

# modification
num[0] = 0
num[1] = 1
print(num)
# inserting
num.insert(2,2)# insert at given postion
num.extend([6,7,8])# it add at end not required for position
num.append(9)# add at end
print(num)

# removing
num.remove(9)# remove by element
num.pop(8)# remove by index
print(num)

# slicing
first_three = num[:3]
middle_two = num[1:3]
last_two = num[-2:]
print(first_three)
print(middle_two)
print(last_two)

# iteration over list
for n in num:
    print(num[n])

# list comprehenstion
square = [x**2 for x in range(6)]
print(square)

# Creating dictinoaries
empty_dic = {}
person = {
    "name" : "Amisha",
    "age" : 30,
    "email" : "kumariamisha2022@gmail.com"
}
print(person)
# Acessing values
name = person["name"]
age = person["age"]
print(name, age)

# modifing values
person["age"] = 31
print(person)
# Adding new key value pair
person["address"] = "vastu vihar"
# remove a key value pair
del person ["email"]
print(person)
# dic method
keys = person.keys()
values = person.values()
items = person.items()
print(keys)
print(values)
print(items)

# iteration
for key in person.keys():
    print(key)

for value in person.values():
    print(value)

for key , value in person.items():
    print(key, ":" , value)