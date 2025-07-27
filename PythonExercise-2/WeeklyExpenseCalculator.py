def calculate_expenses(expenses):
    total = sum(expenses)
    average = total / len(expenses)
    highest = max(expenses)
    return total, average, highest

expenses = []
print("Enter your expenses for 7 days:")
for i in range(7):
    amount = float(input(f"Day {i+1}: "))
    expenses.append(amount)

total, avg, high = calculate_expenses(expenses)
print(f"Total spent: ₹{total:.2f}")
print(f"Average per day: ₹{avg:.2f}")
print(f"Highest spending in a day: ₹{high:.2f}")
