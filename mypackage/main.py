# Importing the arithemtic module from my pacakage
# from mypackage import *
from mypackage import Arithmetic
# using function from arithmetic module
print("Addition :",Arithmetic.add(10,5))
print("division:", Arithmetic.div(20,2))

 # Importing the geometric module from package
from mypackage import Gemoteric
# using function from gemoteric
print("Area of circle:",Gemoteric.area_of_circle(2))
print("Area of rectangle:", Gemoteric.area_of_rectangle(10,2))

# keyword argument example
def describe_pet(pet_name, animal_type="Dog"):
    print(f"I have a {animal_type} named {pet_name}")


# using keyword arguments
describe_pet(animal_type = "cat", pet_name= "micku")
describe_pet(pet_name="Rover") #using animal type as default

# Arbitrary agruments

def make_pizza (size, *toppings):
    print(f"Making a {size}, pizza with the following toppings:")
    for topping in toppings:
        print(f" {topping}")

# Calling with arbitrary positional arguments
make_pizza (12, "peproj","mushrrom","green peppares")

# Arbitary keyword arguments
def build_profiles(first, last , **user_info):
    return {"first_name":first,"last_name":last,**user_info}

# Calling with arbitrary keyword arguments
user_profile = build_profiles("john", "Doe", location= "New york", field= "Engineering")
print(user_profile)