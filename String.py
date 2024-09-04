first_name = "Amisha"
last_name = "Kumari"
# using + operator
Name = "My is " + first_name + " " + last_name
# using f dot
f_name = "{} {}".format(first_name, last_name)
# using f-string
nid = f" My name is {first_name} {last_name}"
print(f_name)
print(nid)
print(Name)
# operation on string
text = " Python programming "
# to trim spaces
strip_text = text.strip()
print(strip_text)
# uppercase string
uppercase_text = text.upper()
print(uppercase_text)
# to check start with
start_with = text.startswith("Python")
print(start_with)
# replay words
replace_text = text.replace("programming", "Coding")
print(replace_text)