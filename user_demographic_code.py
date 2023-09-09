import csv
import random
import string

# Function to generate a random user ID
def generate_user_id():
    return 'U' + ''.join(random.choice(string.digits) for _ in range(4))

# Sample data for the User Demographics dimension table
user_demographics_data = []

for _ in range(100):
    user_id = generate_user_id()
    age = random.randint(18, 65)
    gender = random.choice(["Male", "Female"])
    interests = random.choices(["Music", "Reading", "Gaming", "Travel", "Photography", "Cooking"], k=random.randint(1, 3))
    
    user_demographics_data.append({
        "user_id": user_id,
        "age": age,
        "gender": gender,
        "interests": ";".join(interests)
    })

# Define the CSV file path
csv_file_path = "user_demographics.csv"

# Write the data to a CSV file
with open(csv_file_path, mode='w', newline='') as csv_file:
    fieldnames = ["user_id", "age", "gender", "interests"]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    # Write the header
    writer.writeheader()

    # Write the sample data
    writer.writerows(user_demographics_data)

print(f"CSV file '{csv_file_path}' has been created with User Demographics dimension data.")
