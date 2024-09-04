#Creating a tuple of colors
colors = ("red", "green", "blue")

#Acessing elements of the tuple
print("first color:", colors[0])
print("last color:", colors[-1])

#length of the tuple
tuple_length = len(colors)
print("length of the tuple : ", tuple_length)

# looping through thr tuple
print("tuple elements:")
for color in colors:
    print(color)

# SETS
# creating a set of fruits
fruits = {"apple", "banana", "orange"}

# adding an element to the set
fruits.add("grape")

#removing an element from the set
fruits.remove("banana")

# checking if an element is in the set
print("Is 'apple' in the set?", "apple" in fruits)
print("Is 'banana' in the set?", "banana" in fruits)

# Length of the set
set_length = len(fruits)
print("Number of elements in the set :", set_length)

# looping through the set
print("set elements:")
for fruit in fruits:
    print(fruit)

print(fruits[0])