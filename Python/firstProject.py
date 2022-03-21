first_name = input("Please enter your first name: ")
last_name = input("Please enter your last name: ")
middle_name = input("Please enter your middle name: ")


print(f"If your full name is {first_name} {middle_name} {last_name} then please confirm by writting 'Y', otherwise write 'N''")
answer = input()

if answer == 'Y':
    print("Great have a nice day")

    else:
        print("Oh well that sucks")
