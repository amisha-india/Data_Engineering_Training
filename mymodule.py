# module.py

def greet(name):
    return f"Hello, {name}"

def add(a,b):
    return a+b

def sub(a,b):
    return a - b

def div(a,b):
    return a/b

def mul(a,b):
    return a * b

#Running the module as a script
if __name__== "__main__":
    print("Running it as script for testing")
    name = "world"
    print(greet(name))