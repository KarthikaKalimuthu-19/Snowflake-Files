import string

def is_strong(password):
    has_upper = any(c.isupper() for c in password)
    has_digit = any(c.isdigit() for c in password)
    has_special = any(c in "!@#$" for c in password)
    return has_upper and has_digit and has_special

while True:
    password = input("Enter a strong password: ")
    if is_strong(password):
        print("Password is strong!")
        break
    else:
        print("Password must contain at least 1 uppercase letter, 1 digit, and 1 special character (!@#$). Try again.")
