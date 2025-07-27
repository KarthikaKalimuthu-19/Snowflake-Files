import random

secret = random.randint(1, 50)
attempts = 5

print("Guess the number between 1 and 50. You have 5 chances!")

for i in range(attempts):
    guess = int(input(f"Attempt {i+1}: "))
    if guess == secret:
        print("6Correct! You guessed it!")
        break
    elif guess < secret:
        print("Too low!")
    else:
        print("Too high!")
else:
    print(f"Sorry! The number was {secret}")
