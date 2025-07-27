import math

def cal_bmi(weight, height):
    bmi = weight / math.pow(height, 2)
    return bmi

weight = float(input("Enter your weight in kg: "))
height = float(input("Enter your height in meters: "))

bmi = cal_bmi(weight, height)

print(f"Your BMI is: {bmi:.2f}")

if bmi < 18.5:
    print("Underweight")
elif 18.5 <= bmi < 25:
    print("Normal")
else:
    print("Overweight")
