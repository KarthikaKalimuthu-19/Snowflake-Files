contacts = {}

def add_contact():
    name = input("Enter contact name: ")
    phone = input("Enter phone number: ")
    contacts[name] = phone
    print("Contact added.")

def view_contacts():
    if not contacts:
        print("No contacts to show.")
    else:
        print("\n Contacts:")
        for name, phone in contacts.items():
            print(f"{name}: {phone}")

def save_to_file():
    with open("contacts.txt", "w") as f:
        for name, phone in contacts.items():
            f.write(f"{name},{phone}\n")
    print("Contacts saved to contacts.txt")

while True:
    print("\nMenu:\n1. Add Contact\n2. View Contacts\n3. Save & Exit")
    choice = input("Choose an option (1-3): ")

    if choice == "1":
        add_contact()
    elif choice == "2":
        view_contacts()
    elif choice == "3":
        save_to_file()
        break
    else:
        print("Invalid option. Try again.")
