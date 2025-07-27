import datetime

def calculate_total_avg(marks):
    total = sum(marks)
    average = total / len(marks)
    return total, average

def get_grade(average):
    if average >= 90:
        return "A"
    elif average >= 75:
        return "B"
    else:
        return "C"

name = input("Enter student name: ")
marks = []
for i in range(3):
    mark = float(input(f"Enter marks for Subject {i+1}: "))
    marks.append(mark)

total, avg = calculate_total_avg(marks)
grade = get_grade(avg)
today = datetime.date.today()

print("\n Report Card")
print(f"Name       : {name}")
print(f"Total      : {total}")
print(f"Average    : {avg:.2f}")
print(f"Grade      : {grade}")
print(f"Date       : {today}")
