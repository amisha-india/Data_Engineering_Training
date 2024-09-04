# base class (parent class):
class Animal:
    def __init__(self, name): #constructor
        self.name = name

    def speak(self):#method
        pass

ani = Animal("Bull") #object
ani.speak()

# Derived class (child class) inherting from Animal
class Dog(Animal):
    def speak(self):
        return "woof"

# creating instance of derived classes
dog = Dog("Buddy")

#calling the speak method on instances
print(f"{dog.name} says:{dog.speak()}")

