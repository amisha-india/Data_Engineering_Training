
# Base class
class Animal:
    def __init__(self,name):
        self.name=name
    def speak(self):
        pass

#Derived class(child class) inheriting from animal
class Dog(Animal):
    def speak(self):
        return "woof!"

class Cat(Animal):
    def speak(self):
        return "Meow!!"
#Creating an instances of a derived class
dog=Dog("Buddy")
cat=Cat("Whiskers")
#calling the speak method on instances
print(f"{dog.name} says:{dog.speak()}")
print(f"{cat.name} says:{cat.speak()}")

#Base Class
class Payment:
    def __init__(self,payment_name):
        self.payment_name=payment_name
    def GetPayment(self):
        pass

class Gpay(Payment):
    def GetPayment(self):
        return "Google Gateway"

class PhonePay(Payment):
    def GetPayment(self):
        return "PhonePay Gateway"
class AmazonPay(Payment):
    def GetPayment(self):
        return "AmazonPay way"

pay1=Gpay("You are using")
pay2=PhonePay("You are now using")
pay3=AmazonPay("You are now using")
print(f"{pay1.payment_name} {pay1.GetPayment()}")
print(f"{pay2.payment_name} {pay2.GetPayment()}")
print(f"{pay3.payment_name} {pay3.GetPayment()}")
